package sql.merger.md5;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Stringify {
	private MessageDigest m;
	private static Stringify st = new Stringify();
	
	private Stringify() {
		try {
			m = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}
	
	public static Stringify getInstance() {
		return st;
	}
	
	public String md5HexString(String str) {
		m.update(str.getBytes(),0,str.length());
		String tmp = new BigInteger(1,m.digest()).toString(16).toUpperCase();
		m.reset();
		return tmp;
	}
}
