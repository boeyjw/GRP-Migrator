package sql.merger.md5;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class Stringify {
	private MessageDigest m;
	
	private Stringify() {
		try {
			m = MessageDigest.getInstance("MD5");
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
	}
	
	public static Stringify getInstance() {
		Stringify st = new Stringify();
		
		return st;
	}
	
	public String md5HexString(String str) {
		m.update(str.getBytes(),0,str.length());
		return new BigInteger(1,m.digest()).toString(16).toUpperCase();
	}
}
