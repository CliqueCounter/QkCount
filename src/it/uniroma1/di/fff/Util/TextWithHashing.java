package it.uniroma1.di.fff.Util;

import org.apache.hadoop.io.Text;

public class TextWithHashing extends Text {
	
	
	public TextWithHashing() {
		super();
	}

	public TextWithHashing(byte[] utf8) {
		super(utf8);		
	}

	public TextWithHashing(String string) {
		super(string);
	}

	public TextWithHashing(Text utf8) {
		super(utf8);
	}

	@Override
	public int hashCode() {
		String s = new String(this.getBytes());
		return s.hashCode();

	}

}
