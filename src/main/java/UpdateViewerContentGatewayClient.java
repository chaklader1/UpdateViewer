/**
 * UpdateViewerContentGatewayClient.java 2 Apr 2007
 *
 * Copyright 2007 ACTIV Financial Systems, Inc. All rights reserved.
 * ACTIV PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */
//package com.activfinancial.samples.contentgatewayapi.updateviewer;

import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.activfinancial.contentplatform.contentgatewayapi.ConflationInfo;
import com.activfinancial.contentplatform.contentgatewayapi.ConflationType;
import com.activfinancial.contentplatform.contentgatewayapi.ContentGatewayClient;
import com.activfinancial.contentplatform.contentgatewayapi.ContentGatewayInfo;
import com.activfinancial.contentplatform.contentgatewayapi.FieldListValidator;
import com.activfinancial.contentplatform.contentgatewayapi.GetPattern;
import com.activfinancial.contentplatform.contentgatewayapi.common.*;
import com.activfinancial.contentplatform.contentgatewayapi.consts.*;
import com.activfinancial.middleware.StatusCode;
import com.activfinancial.middleware.activbase.MiddlewareException;
import com.activfinancial.middleware.application.Application;
import com.activfinancial.middleware.fieldtypes.FieldTypeConsts;
import com.activfinancial.middleware.fieldtypes.TRational;
import com.activfinancial.middleware.fieldtypes.Time;
import com.activfinancial.middleware.fieldtypes.timecommon.Resolution;
import com.activfinancial.middleware.service.FileConfiguration;
import com.activfinancial.middleware.service.ServiceApi;
import com.activfinancial.middleware.service.ServiceInstance;
import com.activfinancial.middleware.system.HeapMessage;
import com.activfinancial.middleware.system.RequestId;
//import com.activfinancial.samples.contentgatewayapi.updateviewer.ProgramConfiguration.TimestampFormat;

/**
 * @author Ilya Goberman
 *
 */
public class UpdateViewerContentGatewayClient extends ContentGatewayClient {

    // filed separator
	private String separator;

    // The symbol.
	private List<String> symbolList;

    // List of filed ids
	private List<Integer> fieldIdList;

    // List of filed types
	private List<Short> eventTypeList;

    // output file
	private BufferedOutputStream file;

    // The field list validator.
	private FieldListValidator fieldListValidator;

	private boolean shouldOutputHeader;

    private ProgramConfiguration.TimestampFormat timestampFormat;
    private ProgramConfiguration config;

	public UpdateViewerContentGatewayClient(
			Application application,
			ProgramConfiguration programConfiguration) {
		super(application);

		this.config = programConfiguration;
		this.separator = programConfiguration.getSeparator();
		this.symbolList = programConfiguration.getSymbolList();
		this.fieldIdList = programConfiguration.getFieldIdList();
		this.eventTypeList = programConfiguration.getEventTypeList();
		this.shouldOutputHeader = true;
        this.timestampFormat = programConfiguration.getUpdateTimestampFormat();

		this.fieldListValidator = new FieldListValidator(this);

		if (programConfiguration.getOutputFile() != null && programConfiguration.getOutputFile().length() != 0) {
			try {
				this.file = new BufferedOutputStream(new FileOutputStream(new File(programConfiguration.getOutputFile()), false));
			}
			catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		}

		if (!connect()) {
            application.postDiesToThreads();
			return;
        }

	    // Check conflation is supported if requested.
	    if (!areConflationParametersValid()) {
	        System.out.println("Specified conflation parameters are not supported by the content gateway.");
            application.postDiesToThreads();
            return;
	    }

		outputHeader();

		subscribe();
	}

	private boolean connect() {
		StatusCode statusCode;

        ContentGatewayClient.ConnectParameters connectParameters = new ContentGatewayClient.ConnectParameters();
//        if (config.getHost() != null) {
//            connectParameters.url = String.format("ams://%s/ContentGateway:Service?rxCompression=Rdc", config.getHost());
//        }
//        else {
//            // First stage to connect is to find a service to connect to
//            // we need to resolve a service id (eg "Service.ContentGateway") to a list of service instances with that id.
//            // each instance of a service has a list of access points associated with it.
//    		List<ServiceInstance> serviceInstanceList = new ArrayList<ServiceInstance>();
//
//    		Map<String, Object> attributes = new HashMap<String, Object>();
//    		attributes.put(FileConfiguration.FILE_LOCATION, getApplication().getSettings().serviceLocationIniFile);
//    		statusCode = ServiceApi.findServices(ServiceApi.CONFIGURATION_TYPE_FILE, config.getServiceId(), attributes, serviceInstanceList);
//
//    		if (StatusCode.STATUS_CODE_SUCCESS != statusCode) {
//    			System.out.println("FindServices() failed, error - " + statusCode.toString());
//    			return false;
//    		}
//
//    		// here we are just going to pick the first service that is returned, and its first access point url
//    		ServiceInstance serviceInstance = serviceInstanceList.get(0);
//
//            if (config.getServiceInstanceId() != null) {
//                for (ServiceInstance si : serviceInstanceList) {
//                    if (si.serviceAccessPointList.get(0).id.equals(config.getServiceInstanceId())) {
//                        serviceInstance = si;
//                        break;
//                    }
//                }
//            }
//            connectParameters.url = serviceInstance.serviceAccessPointList.get(0).url;
//        }


		connectParameters.serviceId = "Service.ContentGateway";
		connectParameters.url = "ams://199.47.167.100:9005/ContentGateway:Service?rxCompression=Rdc";
		connectParameters.userId = "drwt1000-user11";
		connectParameters.password = "drwt-u11";

		// first we need to connect to a gateway
		// this is a synchronous connect. Async is also provided
		statusCode = super.connect(connectParameters, DEFAULT_TIMEOUT);

		if (StatusCode.STATUS_CODE_SUCCESS != statusCode)
			System.out.println("Connect() failed, error - " + statusCode.toString());

		return StatusCode.STATUS_CODE_SUCCESS == statusCode;
	}

	public void onConnect() {
		// connection has established.

		// it was, take any action appropriate here

		outputInfoMessage("Connect to gateway is up");
	}

	public void onBreak() {
		// connection has broken.

		// it was, take any action appropriate here

		// depending on the policy chosen, the api will either attempt to reconnect to the same
		// gateway, or look for another one. The onConnect() callback will be invoked when the connection is reestablished

		outputInfoMessage("Connect to gateway has broken");
	}

	private SimpleDateFormat format = new SimpleDateFormat("HH:mm:ss:SSS");

	private void outputInfoMessage(String message) {
		StringBuilder output = new StringBuilder();
		output.append(format.format(new java.util.Date()));
		output.append(separator);
		output.append('I');
		output.append(separator);
		output.append(message);

		if (file != null) {
			try {
				output.append("\n");
				file.write(output.toString().getBytes());
				file.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		else {
			System.out.println(output);
		}
	}

	private void outputHeader() {
		if (this.shouldOutputHeader) {
			if (this.fieldIdList.isEmpty() && (this.fieldListValidator.isInitialized())) {
				for (FieldListValidator.Field field : fieldListValidator) {
					if (FieldIds.FID_SYMBOL != field.fieldId)
						fieldIdList.add(field.fieldId);
				}

				if (!this.fieldIdList.isEmpty())
					this.fieldIdList.add(0, FieldIds.FID_SYMBOL);
			}

			if (!this.fieldIdList.isEmpty()) {
				StringBuilder output = new StringBuilder();
				output.append("Timestamp");
				output.append(this.separator);
				output.append("UpdateType");

				for (Integer fieldId : fieldIdList) {
					output.append(this.separator);
					output.append(getMetaData().getUniversalFieldHelper2(this, fieldId).name);
				}

				if (file != null) {
					try {
						output.append("\n");
						this.file.write(output.toString().getBytes());
						this.file.flush();
					} catch (IOException e) {
						e.printStackTrace();
					}
				}
				else {
					System.out.println(output);
				}

				this.shouldOutputHeader = false;
			}
		}
	}

	private void subscribe() {
	    GetPattern.RequestParameters requestParameters = new GetPattern.RequestParameters();

//		config.setTableNumber(TableNumbers.TABLE_NO_NA_EQUITY_OPTION_ALIAS);
		if (this.symbolList.isEmpty()) {
			requestParameters.symbolPatternList.add(new SymbolId(config.getTableNumber(), "*"));
		}
		else {
            for (String symbolPattern : symbolList) {
                requestParameters.symbolPatternList.add(new SymbolId(config.getTableNumber(), symbolPattern));
	        }
		}

		if (this.eventTypeList.isEmpty()) {
			requestParameters.subscribeParameters.type = SubscribeParameters.Type.TYPE_FULL;
		}
		else {
			requestParameters.subscribeParameters.type = SubscribeParameters.Type.TYPE_EVENT_TYPE_FILTER_INCLUDE_LIST;

			requestParameters.subscribeParameters.eventTypeList = this.eventTypeList;
		}

		requestParameters.requestFlags |= GetPattern.RequestParameters.REQUEST_FLAG_WATCH;
		requestParameters.permissionLevel = (config.isDelayed() ? PermissionLevel.PERMISSION_LEVEL_DELAYED : PermissionLevel.PERMISSION_LEVEL_DEFAULT);
	    requestParameters.conflationParameters = config.getConflationParameters();
		if (config.isIgnoreAlias()) requestParameters.flags |= GetPattern.RequestParameters.FLAG_DONT_RESOLVE_ALIAS;

		RequestBlock requestBlock = new RequestBlock();

		requestBlock.relationshipId = RelationshipIds.RELATIONSHIP_ID_NONE;

		if (this.fieldIdList.isEmpty()) {
			requestBlock.flags |= RequestBlock.FLAG_ALL_FIELDS;
		}
		else if (config.shouldGetSnapshot()) {
			// NOTE: This optimization won't work once support for filtering on subscribed fields is added.
			requestBlock.fieldIdList = this.fieldIdList;
		}

		requestParameters.requestBlockList.add(requestBlock);

		if (!requestParameters.symbolPatternList.isEmpty()) {
			final StatusCode statusCode = getPattern().postRequest(this, RequestId.generateEmptyRequestId(), requestParameters);
			return;
		}
	}

	/**
	 * Called on receiving an asynchronous GetPattern.postRequest() response.<br><br>
	 *
	 *	The response can be deserialized using GetPattern.deserialize() if it is valid.<br>
	 *	The validity of the response can be determined using ContentGatewayClient.isValidResponse().<br>
	 *	If this is the final (or only) part of a response, ContentGatewayClient.isCompleteResponse() will return true.<br>
	 *
	 * @param	response HeapMessage containing the serialized response.
	 */
	public void onGetPatternResponse(HeapMessage response) {
		if (isValidResponse(response)) {
		    GetPattern.ResponseParameters responseParameters = new GetPattern.ResponseParameters();

			if (StatusCode.STATUS_CODE_SUCCESS == getPattern().deserialize(this, response, responseParameters)) {
				if ((this.shouldOutputHeader) || (config.shouldGetSnapshot())) { // NOTE: May need to process one of the original responses to get field id list.
					for (ResponseBlock responseBlock : responseParameters.responseBlockList) {
						if (responseBlock.isValidResponse()) {
							try {
								this.fieldListValidator.initialize(responseBlock.fieldData);
								if (this.shouldOutputHeader)
									outputHeader();
								if (config.shouldGetSnapshot())
									outputSymbol('S', responseBlock.responseKey.symbol, UpdateId.getUnknownUpdateId(), EventType.EVENT_TYPE_NONE, responseBlock.permissionId);
							} catch (MiddlewareException e) {
								e.printStackTrace();
							}
						}
						else {
							System.out.println("This is not valid response");
						}
					}
				}
			}
		}
		else {
		    System.out.println("onGetPatternResponse failed with error code: " + response.getStatusCode());
		}
	}

//	@Override
//	public void onGetPatternResponse(HeapMessage rawMessage)
//	{
//		try
//		{
//			if(rawMessage.getMessageType() == MessageTypes.GATEWAY_REQUEST_GET_PATTERN_EX){
//
//				if (isValidResponse(rawMessage))
//				{
//					GetPattern.ResponseParameters responseParameters = new GetPattern.ResponseParameters();
//
//					StatusCode statusCode = getPattern().deserialize(this, rawMessage, responseParameters);
//					if (statusCode == StatusCode.STATUS_CODE_SUCCESS )
//					{
//						for (ResponseBlock responseBlock : responseParameters.responseBlockList) {
//							try {
//								if (responseBlock.isValidResponse()) {
//									fieldListValidator.initialize(responseBlock.fieldData);
//
//									final String osiSymbol = UsEquityOptionHelper.getOsiSymbolFromSymbol(responseBlock.responseKey.symbol).replaceAll("\\s+", "");
//									System.out.println(osiSymbol+" "+responseBlock.responseKey.symbol);
//								} else {
//
//									System.out.println("Hello");
////									System.out.println("response block is not valid response: " + UsEquityOptionHelper.getOsiSymbolFromSymbol(responseBlock.responseKey.symbol).replaceAll("\\s+", ""));
//								}
//							} catch (Exception ex) {
//								System.out.println("Exception for initiating the field list validator with Activ symbols: " + responseBlock.responseKey.symbol + " and OSI Symbol: " + UsEquityOptionHelper.getOsiSymbolFromSymbol(responseBlock.responseKey.symbol).replaceAll("\\s+", ""));
//							}
//						}
//					}
//					else
//					{
//						System.out.println("invalid snapshot status:" + statusCode );
//					}
//				}
//
//				else {
//
//					System.out.println("snapshot message is not valid response with status: " + rawMessage.getStatusCode() + " and response: " + rawMessage);
//				}
//			}
//		}
//		catch (Exception ex)
//		{
//		}
//	}

	private RecordUpdate recordUpdate = new RecordUpdate();

	/**
	 * Called on receiving a record update.<br><br>
	 *
	 *	The message can be deserialized using RecordUpdateHelper.deserialize().<br>
	 *
	 * @param	update HeapMessage containing the serialized update message.
	 */
	public void onRecordUpdate(HeapMessage update) {
		if (StatusCode.STATUS_CODE_SUCCESS == RecordUpdateHelper.deserialize(this, update, recordUpdate)) {
			try {
				fieldListValidator.initialize(recordUpdate.fieldData);

				if ((this.shouldOutputHeader) && (recordUpdate.isNewRecord()))
					outputHeader();

				if (!this.shouldOutputHeader) {
					char updateType = (recordUpdate.isNewRecord() ? 'A' : recordUpdate.isDelete() ? 'D'  : 'U');
					outputSymbol(updateType, recordUpdate.symbolId.symbol, recordUpdate.updateId, recordUpdate.eventType, recordUpdate.permissionId);
				}
			} catch (MiddlewareException e) {
				e.printStackTrace();
			}
		}
	}

	private void outputSymbol(char updateType, String symbol, char updateId, short eventType, char permissionId) {
		StringBuilder output = new StringBuilder();

		output.append(getTimeStamp());
		output.append(this.separator);
		output.append(updateType);

		for (Integer fieldId : fieldIdList) {
			switch (fieldId) {
				case FieldIds.FID_SYMBOL:
					output.append(this.separator);
					output.append(symbol);
					break;

				case FieldIds.FID_UPDATE_ID:
					output.append(this.separator);
					output.append(Integer.toString(updateId));
					break;

				case FieldIds.FID_EVENT_TYPE:
					output.append(this.separator);
					output.append(Integer.toString(eventType));
					break;

				case FieldIds.FID_PERMISSION_ID:
					output.append(this.separator);
					output.append(Integer.toString(permissionId));
					break;

				default:
					{
						FieldListValidator.Field field = this.fieldListValidator.getField(fieldId);
						if (field != null) {
							output.append(this.separator);
							output.append(fieldToString(field));
						}
						else {
							output.append(this.separator);
						}
					}
					break;
			}
		}

		if (file != null) {
			try {
				output.append("\n");
				this.file.write(output.toString().getBytes());
				this.file.flush();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		else {
			System.out.println(output);
		}
	}

    private double TIMESTAMP_INTERVAL_PRECISION = Math.pow(10, 9);

	private String getTimeStamp() {
        switch (timestampFormat) {
            case TIME_FORMAT_MILLISECONDS:
                return format.format(new java.util.Date());

            case TIME_FORMAT_INTERVAL:
                return String.format("%.6f", (System.nanoTime()) / TIMESTAMP_INTERVAL_PRECISION);
        }
        return null;
    }

    private String fieldToString(FieldListValidator.Field field) {
		switch (field.fieldStatus) {
			case FieldStatus.FIELD_STATUS_DEFINED:
			{
				if (FieldTypeConsts.FIELD_TYPE_TIME == field.fieldType.getType())
					return timeToString(field);
				else if ((FieldTypeConsts.FIELD_TYPE_TRATIONAL != field.fieldType.getType()) || (config.isExtendedTRationalDisplay()))
					return field.fieldType.toString();
				else
					return ((TRational)(field.fieldType)).getRational().toString();
			}

			case FieldStatus.FIELD_STATUS_NOT_PERMISSIONED:
				return "???";

			case FieldStatus.FIELD_STATUS_UNDEFINED:
				return "---";

			default:
				return "";
		}
	}

    private String timeToString(FieldListValidator.Field field) {
    	try {
	    	switch (config.getFieldTimestampFormat()) {
				case TIME_FORMAT_SECONDS:
				case TIME_FORMAT_MILLISECONDS:
				case TIME_FORMAT_MICROSECONDS:
				case TIME_FORMAT_NANOSECONDS:
				{
					final Resolution resolution = getTimeResolution(config.getFieldTimestampFormat());
					return ((Time)(field.fieldType)).toString(resolution);
				}

				case TIME_FORMAT_RECIEVED:
					return ((Time)(field.fieldType)).toString();

				case TIME_FORMAT_SMART:
				{
					final Resolution resolution = ((Time)(field.fieldType)).getResolution();

					HashMap<Integer, Resolution> fidToResolutionMap = (HashMap<Integer, Resolution>) config.getFidToResolutionMap();

					Resolution bestResolution = fidToResolutionMap.containsKey(field.fieldId) ? fidToResolutionMap.get(field.fieldId) : null;

					final int isHigherResolution = (bestResolution != null) ? resolution.compareTo(bestResolution) : 0;

					if (isHigherResolution >= 0) {
						bestResolution = resolution;
						fidToResolutionMap.put(field.fieldId, bestResolution);
					}

					return ((Time)(field.fieldType)).toString(bestResolution);
				}

				default:
					throw new MiddlewareException(StatusCode.STATUS_CODE_INVALID_PARAMETER);
			}
    	} catch (MiddlewareException e) {
			System.out.println(e.toString());
		}

    	return "";
    }

	private Resolution getTimeResolution(ProgramConfiguration.TimestampFormat timestampFormat) throws MiddlewareException {
    	switch (timestampFormat) {
    		case TIME_FORMAT_SECONDS:
    			return Resolution.RESOLUTION_SECOND;

    		case TIME_FORMAT_MILLISECONDS:
    			return Resolution.RESOLUTION_MILLISECOND;

    		case TIME_FORMAT_MICROSECONDS:
    			return Resolution.RESOLUTION_MICROSECOND;

    		case TIME_FORMAT_NANOSECONDS:
    			return Resolution.RESOLUTION_NANOSECOND;

    		default:
    			throw new MiddlewareException(StatusCode.STATUS_CODE_INVALID_PARAMETER);
    	}
    }

    private boolean areConflationParametersValid() {
        if (!isConflationEnabled())
            return true;

        ContentGatewayInfo contentGatewayInformation = new ContentGatewayInfo();
        StatusCode statusCode = getContentGatewayInfo(contentGatewayInformation);

        if (StatusCode.STATUS_CODE_SUCCESS == statusCode) {
            ConflationInfo info = contentGatewayInformation.conflationInfo;
            boolean isConflationTypeSupported = (
                info.conflationTypeList.contains(config.getConflationParameters().type));

            boolean isConflationIntervalSupported = (
                info.conflationIntervalList.contains(config.getConflationParameters().interval));

            if (ConflationType.CONFLATION_TYPE_NONE != config.getConflationParameters().type)
                return isConflationTypeSupported && isConflationIntervalSupported;

            if ((!info.isDynamicConflationAvailable) && (config.getConflationParameters().shouldEnableDynamicConflation))
                return false;
        }
        return true;
    }

    private boolean isConflationEnabled() {
        return ((ConflationType.CONFLATION_TYPE_NONE != config.getConflationParameters().type) ||
                (config.getConflationParameters().shouldEnableDynamicConflation));
    }
}
