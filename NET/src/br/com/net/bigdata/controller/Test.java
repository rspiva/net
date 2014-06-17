package br.com.net.bigdata.controller;

import java.util.List;

import br.com.net.bigdata.model.AccessLog;

public class Test {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		CdrController cc = new CdrController();
		cc.getListAccessLog();
		cc.writesRecords();
	}

}
