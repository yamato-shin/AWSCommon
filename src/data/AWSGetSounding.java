package data;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLConnection;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.ComparisonOperator;
import com.amazonaws.services.dynamodbv2.model.Condition;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;

import constant.Constant;

public class AWSGetSounding {
	
	/** base URL to get sounding data */
	private String _baseUrl = "";
	/** region of data */
	private String _region = "";
	/** data type to get raw data */
	private String _getDataType = "";
	/** data type to get image */
	private String _getImageType = "";
	/** year of data */
	private String _getYear = "";
	/** month of data */
	private String _getMonth = "";
	/** date/hour of data (DDHH24) */
	private String _getDateHour = "";
	/** station name defined by Wyoming University */
	private String _station = "";

	/**
	 * constructor
	 * @param region	region (ex. south east asia: seasia)
	 * @param year		year (ex.2014)
	 * @param month		month (ex.02)
	 * @param fromDate	DDHH24 (ex. 2014/02/09 01:02:03 -> 0901)
	 * @param toDate	DDHH24 (ex. 2014/02/09 01:02:03 -> 0901)
	 * @param station	station name (ex. Tateno: 47646)
	 * @throws IOException
	 */
	public AWSGetSounding(String region, String year, String month, String dateHour, String station) throws IOException {

		// load property file
		Properties prop = new Properties();
		InputStream input = AWSGetSounding.class.getClassLoader().getResourceAsStream("config.properties");
		prop.load(input);
		
		//----------------------------------------------
		// set base URL (URL of Wyoming University)
		//----------------------------------------------
		_baseUrl = prop.getProperty("sounding.baseUrl");
		
		//----------------------------------------------
		// set type
		//----------------------------------------------
		_getDataType = prop.getProperty("sounding.getDataFileType");

		//----------------------------------------------
		// set region
		//----------------------------------------------
		_region = region;

		//----------------------------------------------
		// other parameters
		//----------------------------------------------
		_getYear = year;
		_getMonth = month;
		_getDateHour = dateHour;
		_station = station;
		
		input.close();
	}
	
	/**
	 * method to get sounding data
	 * @return HashMap { SOUNDING_ID, GROUND_DEW_POINT }
	 * @throws FileNotFoundException
	 * @throws IOException
	 */
	public HashMap<String,String> getSounding() throws FileNotFoundException, IOException {
		
		// Get Amazon DynamoDB client
		AmazonDynamoDBClient client = DynamoDbAccess.getClient();

		// make master record if data does not exist
		String getDate = _getYear + _getMonth + _getDateHour;
		String soundingId = _station + "_" + getDate;
		
		//------------------------------------------
		// check if there exists detail records
		//------------------------------------------
		// Amazon DynamoDB search
		Map<String, Condition> keyConditionsDtl = new HashMap<String, Condition>();

		Condition hashKeyConditionDtl = new Condition()
		.withComparisonOperator(ComparisonOperator.EQ)
		.withAttributeValueList(new AttributeValue().withS(soundingId));
		keyConditionsDtl.put("soundingId", hashKeyConditionDtl);

		Condition rangeKeyConditionDtl = new Condition()
		.withComparisonOperator(ComparisonOperator.EQ)
		.withAttributeValueList(new AttributeValue().withS("001"));
		keyConditionsDtl.put("recordId", rangeKeyConditionDtl);

		QueryRequest qReqDtl = new QueryRequest()
		.withTableName("SoundingDtl")
		.withKeyConditions(keyConditionsDtl)
		.withAttributesToGet(Arrays.asList("recordId", "dewpoint"))
		.withConsistentRead(true);

		QueryResult qRsltDtl = client.query(qReqDtl);

		String gndDwPt = "";	// Dew point at Ground
		if (qRsltDtl.getItems().size() == 0) {
			//------------------------------------------
			// get sounding data from Wyoming University
			//------------------------------------------
			StringBuffer urlBuf
			= new StringBuffer(_baseUrl)
			.append("?region=").append(_region)		// region
			.append("&TYPE=").append(_getDataType)	// data type
			.append("&YEAR=").append(_getYear)		// year
			.append("&MONTH=").append(_getMonth)	// month
			.append("&FROM=").append(_getDateHour)	// date/hour from
			.append("&TO=").append(_getDateHour)	// date/hour to
			.append("&STNM=").append(_station);		// station name

			URL url = new URL(urlBuf.toString());
			URLConnection con = url.openConnection();
			con.setConnectTimeout(15000);
			con.setReadTimeout(15000);

			BufferedReader br = new BufferedReader(new InputStreamReader(con.getInputStream()));

			//------------------------------------------
			// make detail record
			//------------------------------------------
			String line = "";
			int cnt = 0;
			int recordId = 0;
			while((line = br.readLine()) != null) {

				// data start at 11th line
				//  1: <HTML>
				//  2: <TITLE>University of Wyoming - Radiosonde Data</TITLE>
				//  3: <LINK REL="StyleSheet" HREF="/resources/select.css" TYPE="text/css">
				//  4: <BODY BGCOLOR="white">
				//  5: <H2>53463 ZBHH Hohhot Observations at 00Z 09 Feb 2014</H2>
				//  6: <PRE>
				//  7: -----------------------------------------------------------------------------
				//  8:   PRES   HGHT   TEMP   DWPT   RELH   MIXR   DRCT   SKNT   THTA   THTE   THTV
				//  9:    hPa     m      C      C      %    g/kg    deg   knot     K      K      K 
				// 10: -----------------------------------------------------------------------------
				if(++cnt < 11) {
					continue;
				}

				// end of data
				if(line.startsWith("</PRE>")) {
					break;
				}

				//--------------------------------------
				// read and set data
				//--------------------------------------
				++recordId;
				String pressure		= line.substring(0,7).trim();
				String height		= line.substring(7,14).trim();
				String temperature	= line.substring(14,21).trim();
				String dewpoint		= line.substring(21,28).trim();
				
				// if data is not specified, use "N/A"
				pressure = "".equals(pressure) ? "N/A" : pressure;
				height = "".equals(height) ? "N/A" : height;
				temperature = "".equals(temperature) ? "N/A" : temperature;
				dewpoint = "".equals(dewpoint) ? "N/A" : dewpoint;
				// set dew point at ground
				if (recordId == 1) {
					gndDwPt = dewpoint;
				}
				// set required ground temperature to reach the height
				String reqGndTmp = getReqGndTmp(height, temperature, gndDwPt);

				// Put data to AmazonDynamoDB
				Map<String, AttributeValue> dtlItem = new HashMap<String, AttributeValue>();
				String fmtRecordId = "00" + String.valueOf(recordId);
				fmtRecordId = fmtRecordId.substring(fmtRecordId.length() - 3);
				dtlItem.put("soundingId", new AttributeValue().withS(soundingId));
				dtlItem.put("recordId", new AttributeValue().withS(fmtRecordId));
				dtlItem.put("pressure", new AttributeValue().withS(pressure));
				dtlItem.put("height", new AttributeValue().withS(height));
				dtlItem.put("temperature", new AttributeValue().withS(temperature));
				dtlItem.put("dewpoint", new AttributeValue().withS(dewpoint));
				dtlItem.put("reqGndTmp", new AttributeValue().withS(reqGndTmp));

				PutItemRequest dtlReq = new PutItemRequest().withTableName("SoundingDtl").withItem(dtlItem);
				client.putItem(dtlReq);
			}
		}
		else {
			gndDwPt = qRsltDtl.getItems().get(0).get("dewpoint").getS();
		}

		//------------------------------------------
		// check if there exists a master record
		//------------------------------------------
		// Amazon DynamoDB search
		Map<String, Condition> keyConditionsMst = new HashMap<String, Condition>();
		
		Condition hashKeyConditionMst = new Condition()
			.withComparisonOperator(ComparisonOperator.EQ)
			.withAttributeValueList(new AttributeValue().withS(_station));
		keyConditionsMst.put("station", hashKeyConditionMst);
		
		Condition rangeKeyConditionMst = new Condition()
			.withComparisonOperator(ComparisonOperator.EQ)
			.withAttributeValueList(new AttributeValue().withS(_getYear + _getMonth + _getDateHour));
		keyConditionsMst.put("date", rangeKeyConditionMst);
		
		QueryRequest qReqMst = new QueryRequest()
			.withTableName("SoundingMst")
			.withKeyConditions(keyConditionsMst)
			.withAttributesToGet(Arrays.asList("soundingId"))
			.withConsistentRead(true);
		
		QueryResult qRsltMst = client.query(qReqMst);

		if (qRsltMst.getItems().size() == 0) {
			//------------------------------------------
			// make master record
			//------------------------------------------
			Map<String, AttributeValue> mstItem = new HashMap<String, AttributeValue>();
			mstItem.put("station", new AttributeValue().withS(_station));
			mstItem.put("date", new AttributeValue().withS(getDate));
			mstItem.put("soundingId", new AttributeValue().withS(soundingId));
			mstItem.put("gndDwPt", new AttributeValue().withS(gndDwPt));

			PutItemRequest mstReq = new PutItemRequest().withTableName("SoundingMst").withItem(mstItem);
			client.putItem(mstReq);
		}
		
		HashMap<String,String> retMap = new HashMap<String,String>();
		retMap.put("SOUNDING_ID", soundingId);
		retMap.put("GROUND_DEW_POINT", gndDwPt);
		
		return retMap;
	}
	
	/**
	 * method to calculate required ground temperature to reach to the height at the measured point
	 * @param height		height at the measured point
	 * @param temperature	temperature at the measured point
	 * @param gndDwPt		dew point at ground
	 * @return				required ground temperature to reach to the height at the measured point
	 */
	private String getReqGndTmp(String height, String temperature, String gndDwPt) {
		if ("N/A".equals(height) || "N/A".equals(temperature) || "N/A".equals(gndDwPt)) {
			return "N/A";
		}
		
		double dblHeight = Double.parseDouble(height);
		double dblTemperature = Double.parseDouble(temperature);
		double dblGndDwPt = Double.parseDouble(gndDwPt);
		
		double reqGndTmp = 0.0f;
		
		// If temperature at measured point is lower than dew point at ground, use saturated adiabatic lapse rate
		if (dblTemperature < dblGndDwPt) {
			double heightAtDwPt = dblHeight - (dblGndDwPt - dblTemperature)/Constant.SATURATED_ADIABATIC_LAPSE_RATE * 100.0f;
			reqGndTmp = dblGndDwPt + (heightAtDwPt * Constant.DRY_ADIABATIC_LAPSE_RATE / 100.0f);
		}
		// Else use dry adiabatic lapse rate
		else {
			reqGndTmp = dblTemperature + (dblHeight * Constant.DRY_ADIABATIC_LAPSE_RATE / 100.0f);
		}
		
		return String.valueOf(reqGndTmp);
	}
}
