/**
 * UpdateViewer.java  May 2, 2007
 * 
 * Copyright 2007 ACTIV Financial Systems, Inc. All rights reserved.
 * ACTIV PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

//package com.activfinancial.samples.contentgatewayapi.updateviewer;

import com.activfinancial.middleware.application.Application;
import com.activfinancial.middleware.application.Settings;

/**
 * @author Ilya Goberman
 */
public class UpdateViewer {
	/**
	 * main entry point into application 
	 * @param args
	 */
	public static void main(String[] args) {
		new UpdateViewer().run(args);
	}

	@SuppressWarnings("unused")
	private UpdateViewerContentGatewayClient client;
	
    // Activ application instance
	private void run(String[] args) {
		// parse user supplied arguments.
		ProgramConfiguration programConfiguration = new ProgramConfiguration();
        try {
            if (!programConfiguration.process(args)) {
                return;
            }
        } catch (Exception e) {
            System.err.println(e.getMessage());
            return;
        }
		
		// construct application settings
		Settings settings = new Settings();
		settings.serviceLocationIniFile = programConfiguration.getServiceLocationIniFile();
		
		// construct new application.
		final Application application = new Application(settings);
		
		// start a thread to run the application. Async callbacks will be processed by this thread
		application.startThread();
		
		client = new UpdateViewerContentGatewayClient(
				application, 
				programConfiguration); 
	}
}
