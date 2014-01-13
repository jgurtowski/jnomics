package edu.cshl.schatz.jnomics.util;

import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ShockUtil {

	final static Logger logger = LoggerFactory.getLogger(FileUtil.class);
	public static void setHttpProxy (String http_proxy) throws IOException {
		if(null != http_proxy && http_proxy.contains(":")){
			String[] arr = http_proxy.split(":");
			if(arr.length != 2)
				throw new IOException("Cannot interpret http-proxy in conf");
			System.setProperty("https.proxyHost", arr[0]);
			System.setProperty("https.proxyPort", arr[1]);
			logger.info("https.proxyHost :" + System.getProperty("https.proxyHost"));
			logger.info("https.proxyPort :" + System.getProperty("https.proxyPort"));
		}
	}
}
