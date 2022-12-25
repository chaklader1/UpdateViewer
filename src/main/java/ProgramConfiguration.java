/**
 * ProgramConfiguration.java  Mar 16, 2007
 *
 * Copyright 2007 ACTIV Financial Systems, Inc. All rights reserved.
 * ACTIV PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

//package com.activfinancial.samples.contentgatewayapi.updateviewer;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.activfinancial.contentplatform.contentgatewayapi.ConflationParameters;
import com.activfinancial.contentplatform.contentgatewayapi.ConflationType;
import com.activfinancial.middleware.StatusCode;
import com.activfinancial.middleware.activbase.MessageValidator;
import com.activfinancial.middleware.activbase.MiddlewareException;
import com.activfinancial.middleware.fieldtypes.timecommon.Resolution;
import com.activfinancial.samples.common.busobj.CommandLineParser;
import com.activfinancial.samples.common.busobj.ConfigurationParameter;


/**
 * @author Ilya Goberman
 */
public class ProgramConfiguration {


    enum TimestampFormat {
		TIME_FORMAT_SECONDS,
        TIME_FORMAT_MILLISECONDS,
		TIME_FORMAT_MICROSECONDS,
		TIME_FORMAT_NANOSECONDS,
		TIME_FORMAT_INTERVAL,
		TIME_FORMAT_RECIEVED,
		TIME_FORMAT_SMART,
		TIME_FORMAT_UNDEFINED;

    	enum TimestampInitials {
			S,
			M,
			U,
			N,
			I,
			R,
			T
		}

    	public static TimestampFormat fromString(String property) throws MiddlewareException {
    		if (property == null)
    			return TIME_FORMAT_MILLISECONDS;

    		TimestampInitials timestampInitial = TimestampInitials.valueOf(property.toUpperCase());

			switch (timestampInitial) {
				case S:
					return TIME_FORMAT_SECONDS;

				case M:
					return TIME_FORMAT_MILLISECONDS;

				case U:
					return TIME_FORMAT_MICROSECONDS;

				case N:
					return TIME_FORMAT_NANOSECONDS;

				case I:
					return TIME_FORMAT_INTERVAL;

				case R:
					return TIME_FORMAT_RECIEVED;

				case T:
					return TIME_FORMAT_SMART;

				default:
					throw new MiddlewareException(StatusCode.STATUS_CODE_INVALID_PARAMETER);
			}
		}
	}

	ProgramConfiguration() {
		this.fidToResolutionMap = new HashMap<Integer, Resolution>();
	}

/*
        connectParameters.serviceId = "Service.ContentGateway";
        connectParameters.url = "ams://199.47.167.100:9005/ContentGateway:Service?rxCompression=Rdc";
        connectParameters.userId = "drwt1000-user01";
        connectParameters.password = "drwt-u01";
* */

	boolean process(String[] args) throws Exception {
		List<ConfigurationParameter> parameters = new ArrayList<ConfigurationParameter>();

		// mandatory
		parameters.add(new ConfigurationParameter("U", "drwt1000-user11","ACTIV feed user id", "setUserId"));
		parameters.add(new ConfigurationParameter("P", "drwt-u11","ACTIV feed password", "setPassword"));

		// optional
		parameters.add(new ConfigurationParameter("S", "META.*", "Symbol pattern to subscribe to", "setSymbolList"));
		parameters.add(new ConfigurationParameter("O", "", "File to log updates to", "setOutputFile"));
		parameters.add(new ConfigurationParameter("C", ",", "Character to use as field separator in output file", "setSeparator"));
		parameters.add(new ConfigurationParameter("F", "*", "Semi-colon delimited list of field ids", "setFieldIdList"));
		parameters.add(new ConfigurationParameter("T", "0", "Table number to subscribe to", "setTableNumber"));
//		parameters.add(new ConfigurationParameter("T", "0", "Table number to subscribe to", "setTableNumber"));
		parameters.add(new ConfigurationParameter("L", "/Users/chaklader/IdeaProjects/UpdateViewer/ServiceLocation.xml", "Service location ini file to use", "setServiceLocationIniFile"));
		parameters.add(new ConfigurationParameter("N", null, "Network to connect to", "setServiceInstanceId"));
		parameters.add(new ConfigurationParameter("I", "Service.ContentGateway", "Service id to connect to", "setServiceId"));
		parameters.add(new ConfigurationParameter("V", "*", "Semi-colon delimited list of event types to subscribe to", "setEventTypeList"));
		parameters.add(new ConfigurationParameter("update-timestamp-format", "M", "The timestamp format, M = milliseconds, I = interval timer", "setUpdateTimestampFormat"));
		parameters.add(new ConfigurationParameter("field-timestamp-format", "T", "The field timestamp format, S = seconds, M = milliseconds, U = microseconds, N = nanoseconds, R = received, T = smart", "setFieldTimestampFormat"));
		parameters.add(new ConfigurationParameter("H", null, "Host to connect to with optional port i.e. hostname[:9002] or a.b.c.d[:9002]", "setHost"));

		parameters.add(new ConfigurationParameter("D", "false", "Subscribe to delayed feed rather than realtime", "setDelayed"));
		parameters.add(new ConfigurationParameter("E", "false", "Whether to display trending information with TRational fields", "setExtendedTRationalDisplay"));
		parameters.add(new ConfigurationParameter("A", "false", "Don't resolve aliases to their target symbols", "setIgnoreAlias"));
		parameters.add(new ConfigurationParameter("G", "true", "Get an initial snapshot of fields in requested symbols rather than just subscribing to updates", "setShouldGetSnapshot"));

		parameters.add(new ConfigurationParameter("conflation-type", "NONE", "The type of conflation to use", "setConflationType"));
		parameters.add(new ConfigurationParameter("conflation-interval", "0", "Conflation interval to use, in milliseconds. Interval must be available on the CG.", "setConflationInterval"));
		parameters.add(new ConfigurationParameter("dynamic-conflation", "false", "Use dynamic conflation.", "setUseDynamicConflation"));

		if (args.length == 0) {
			for (ConfigurationParameter parameter : parameters) {
				CommandLineParser.parse(this, parameter);
			}
			return true;
		}
		else if (args.length == 1 && (args[0].equals("-?") || args[0].equals("/?"))) {
			CommandLineParser.displayHelp(parameters);
			return false;
		}
		else {
			throw new Exception("Unknown command line option.");
		}
	}

	// The service location ini file.
	private String serviceLocationIniFile;

	// The service id.
	private String serviceId;

	// id of a server as defined in the ServiceLocation.xml
	private String serviceInstanceId;

	private String userId = "drwt1000-user11";
	private String password = "drwt-u11";

	// The table number.
	private char tableNumber;

	// Indicates whether the updates should be delayed.
	private boolean isDelayed;

	// Indicates whether the extended trended rational information should be displayed.
	private boolean isExtendedTRationalDisplay;

	// filed separator
	private String separator;

	// file to generate data to
	private String outputFile;

	// Indicates whether the alias is ignored.
	private boolean isIgnoreAlias;

	private boolean shouldGetSnapshot = true;

	// The symbol.
	private List<String> symbolList;

	private List<Integer> fieldIdList;

	private List<Short> eventTypeList;

	private TimestampFormat updateTimestampFormat;

	private TimestampFormat fieldTimestampFormat;

	private Map<Integer, Resolution> fidToResolutionMap;

	private ConflationParameters conflationParameters = new ConflationParameters();

 	// The host to connect to.
	private String host;

	public boolean setFieldIdList(String fieldSpecificationListString) {
		fieldIdList = new ArrayList<Integer>();
		if (!fieldSpecificationListString.equals("*")) {
			MessageValidator messageValidator = new MessageValidator();

			try {
				final byte[] fieldSpecificationListBytes = fieldSpecificationListString.getBytes();

				for (int offset = 0, lastOffset = 0; offset < fieldSpecificationListString.length(); ++offset) {
					for (; (offset < fieldSpecificationListString.length()) && (':' != fieldSpecificationListString.charAt(offset)) && (';' != fieldSpecificationListString.charAt(offset)); ++offset);

					if (offset <= lastOffset)
						return false;

					messageValidator.initialize(fieldSpecificationListBytes, offset - lastOffset, 0, lastOffset);

					int fieldId = messageValidator.validateUnsignedAsciiIntegralShort(offset - lastOffset);

					lastOffset = offset + 1;

					fieldIdList.add(fieldId);
				}
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}

		return true;
	}

	public boolean setEventTypeList(String eventListString) {
		eventTypeList = new ArrayList<Short>();
		if (!eventListString.equals("*")) {
			MessageValidator messageValidator = new MessageValidator();

			try {
				final byte[] fieldSpecificationListBytes = eventListString.getBytes();

				for (int offset = 0, lastOffset = 0; offset < eventListString.length(); ++offset) {
					for (; (offset < eventListString.length()) && (':' != eventListString.charAt(offset)) && (';' != eventListString.charAt(offset)); ++offset);

					if (offset <= lastOffset)
						return false;

					messageValidator.initialize(fieldSpecificationListBytes, offset - lastOffset, 0, lastOffset);

					short fieldId = (short) messageValidator.validateUnsignedAsciiIntegralShort(offset - lastOffset);

					lastOffset = offset + 1;

					eventTypeList.add(fieldId);
				}
			}
			catch (Exception e) {
				e.printStackTrace();
			}
		}

		return true;
	}

	public void setSymbolList(String symbolListString) {
		this.symbolList = new ArrayList<String>();
		if (!"*".equals(symbolListString)) {
			boolean fileFound = true;

			BufferedReader file = null;
			try {
				file = new BufferedReader(new FileReader(symbolListString));
			} catch (FileNotFoundException e) {
				fileFound = false;
			}

			if (fileFound) {
				String symbol;

				try {
					while ((symbol = file.readLine()) != null) {
						if (!symbol.equals("")) {
							symbolList.add(symbol);
						}
					}
				} catch (IOException e) {
					e.printStackTrace();
				}

				if (symbolList.isEmpty())
					return;
			}
			else {
				for (int offset = 0, lastOffset = 0; offset <= symbolListString.length(); ++offset) {
					StringBuilder symbol = new StringBuilder();

					boolean isEscaped = false;

					for (; (offset < symbolListString.length()) && ((';' != symbolListString.charAt(offset)) || ((0 != offset) && ('\\' == symbolListString.charAt(offset-1)))); ++offset) {
						if (isEscaped) {
							symbol.append(symbolListString.charAt(offset));
							isEscaped = false;
						}
						else if ('\\' == symbolListString.charAt(offset)) {
							isEscaped = true;
						}
						else {
							symbol.append(symbolListString.charAt(offset));
						}
					}

					if (offset <= lastOffset)
						return;

					lastOffset = offset + 1;

					symbolList.add(symbol.toString());
				}
			}
		}
	}

	public boolean shouldGetSnapshot() {
		return shouldGetSnapshot;
	}
    public void setShouldGetSnapshot(boolean shouldGetSnapshot) {
        this.shouldGetSnapshot = shouldGetSnapshot;
    }

	public boolean isDelayed() {
		return isDelayed;
	}

    public void setDelayed(boolean isDelayed) {
        this.isDelayed = isDelayed;
    }

	public boolean isExtendedTRationalDisplay() {
		return isExtendedTRationalDisplay;
	}

	public void setExtendedTRationalDisplay(boolean isExtendedTRationalDisplay) {
		this.isExtendedTRationalDisplay = isExtendedTRationalDisplay;
	}

	public boolean isIgnoreAlias() {
		return isIgnoreAlias;
	}

	public void setIgnoreAlias(boolean isIgnoreAlias) {
		this.isIgnoreAlias = isIgnoreAlias;
	}

	public String getPassword() {
		return password;
	}

	public void setPassword(String password) {
		this.password = password;
	}

	public String getServiceId() {
		return serviceId;
	}

	public void setServiceId(String serviceId) {
		this.serviceId = serviceId;
	}

	public String getServiceLocationIniFile() {
		return serviceLocationIniFile;
	}

	public void setServiceLocationIniFile(String serviceLocationIniFile) {
		this.serviceLocationIniFile = serviceLocationIniFile;
	}

	public char getTableNumber() {
		return tableNumber;
	}

	public void setTableNumber(char tableNumber) {
		this.tableNumber = tableNumber;
	}

	public String getUserId() {
		return userId;
	}

	public void setUserId(String userId) {
		this.userId = userId;
	}

	public String getOutputFile() {
		return outputFile;
	}

	public void setOutputFile(String outputFile) {
		this.outputFile = outputFile;
	}

	public String getSeparator() {
		return separator;
	}

	public void setSeparator(String separator) {
		this.separator = separator;
	}

	public List<String> getSymbolList() {
		return symbolList;
	}

	public List<Short> getEventTypeList() {
		return eventTypeList;
	}

	public List<Integer> getFieldIdList() {
		return fieldIdList;
	}

	public String getServiceInstanceId() {
		return serviceInstanceId;
	}

	public void setServiceInstanceId(String serviceInstanceId) {
		this.serviceInstanceId = serviceInstanceId;
	}

	public TimestampFormat getUpdateTimestampFormat() {
		return updateTimestampFormat;
	}

	public void setUpdateTimestampFormat(String format) throws MiddlewareException {
		this.updateTimestampFormat = TimestampFormat.fromString(format);
	}

	public TimestampFormat getFieldTimestampFormat() {
		return fieldTimestampFormat;
	}

	public void setFieldTimestampFormat(String format) throws MiddlewareException {
		this.fieldTimestampFormat = TimestampFormat.fromString(format);
	}

	public Map<Integer, Resolution> getFidToResolutionMap() {
		return fidToResolutionMap;
	}

	public void setFidToResolutionMap(Map<Integer, Resolution> map) {
		this.fidToResolutionMap = map;
	}

	public String getHost() {
		return host;
	}

	public void setHost(String host) {
		if (host != null && host.length() != 0) {
			this.host = host + ((host.indexOf(":") != -1) ? "" : ":9002");
		}
	}

	public void setConflationType(String conflationType) {
		if (conflationType.equals("NONE"))
			conflationParameters.type = ConflationType.CONFLATION_TYPE_NONE;
		else if (conflationType.equals("QUOTE"))
			conflationParameters.type = ConflationType.CONFLATION_TYPE_QUOTE;
		else if (conflationType.equals("TRADE"))
			conflationParameters.type = ConflationType.CONFLATION_TYPE_TRADE;
	}

	public void setConflationInterval(int interval) {
		conflationParameters.interval = interval;
	}

	public void setUseDynamicConflation(boolean shouldEnableDynamicConflation) {
		conflationParameters.shouldEnableDynamicConflation = shouldEnableDynamicConflation;
	}

	public ConflationParameters getConflationParameters() {
		return conflationParameters;
	}
}
