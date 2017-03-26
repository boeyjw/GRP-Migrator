package sql.merger.md5;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

/**
 * Singleton class for hashing strings into MD5.
 *
 */
public class Md5String {
	private MessageDigest m;
	private static Md5String st = new Md5String();
	
	private Md5String() {
		try {
			m = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Get an instance of this class.
	 * @return An instant of this class.
	 */
	public static Md5String getInstance() {
		return st;
	}
	
	/**
	 * Converts string into MD5 strings and digest it into readable MD5 string.
	 * @param str The input string to be converted to MD5.
	 * @return Sring representation of MD5 of the string inputted.
	 */
	public String md5HexString(String str) {
		m.update(str.getBytes(),0,str.length());
		String tmp = new BigInteger(1,m.digest()).toString(16).toUpperCase();
		m.reset();
		return tmp;
	}
}
