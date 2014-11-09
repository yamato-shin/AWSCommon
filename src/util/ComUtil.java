package util;

public class ComUtil {
	/**
	 * method to check if given string is blank or not
	 * @param str
	 * @return
	 */
	public static boolean isBlank(String str) {
		if(str == null || "".equals(str)) {
			return true;
		}
		else {
			return false;
		}
	}
 
}
