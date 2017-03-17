package sql.merger.md5;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

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
	
	public static Md5String getInstance() {
		return st;
	}
	
	public String md5HexString(String str) {
		m.update(str.getBytes(),0,str.length());
		String tmp = new BigInteger(1,m.digest()).toString(16).toUpperCase();
		m.reset();
		return tmp;
	}
}
