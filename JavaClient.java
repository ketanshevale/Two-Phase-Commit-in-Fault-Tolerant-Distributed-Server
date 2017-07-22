
import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TIOStreamTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class JavaClient {
	public static List<File> fileInAFolder = new ArrayList<File>();

	public static void main(String[] args) {
		TTransport transport;
		/**
		 * Arguments for the thrift has been specified over here
		 */
		String hostName = null;
		int clientPortNumber;
		String operation = null;
		String operationType = null;
		String fileName = null;
		String operationFileName = null;

		if (args.length != 6) {
			System.err.println(
					"Please provide arguments in  Following format: \n<hostName> <portNumber> --operation <operationName> --filename <fileName>");
			System.exit(1);
		}

		hostName = args[0].trim().toLowerCase();
		clientPortNumber = Integer.parseInt(args[1].trim());
		operation = args[2].trim().toLowerCase();

		operationType = args[3].trim().toLowerCase();
		if (args.length == 6) {
			fileName = args[4].trim().toLowerCase();
			operationFileName = args[5].trim().toLowerCase();
		}
		try {
			transport = new TSocket(hostName, clientPortNumber);
			transport.open();
			TProtocol protocol = new TBinaryProtocol(transport);
			FileStore_Cordinator.Client client = new FileStore_Cordinator.Client(protocol);
			perform(client, operationType, operationFileName);
			transport.close();
		} catch (Exception e) {
			SystemException systemException = new SystemException();
			systemException.setMessage(
					"Connection refused occurred while opening the code . Check the server if its ready or the <portNumber>");
			System.exit(1);
		}

	}

	public static void perform(FileStore_Cordinator.Client client, String operationType, String operationFileName) {
		TIOStreamTransport transport = new TIOStreamTransport(System.out);
		TProtocol binaryProtocol = new TJSONProtocol.Factory().getProtocol(transport);
		if(operationType.equalsIgnoreCase("read")) {
			try {
				RFile rFile = client.readFile(operationFileName);
				rFile.write(binaryProtocol);
			}  catch (SystemException e1) {
				try {
					e1.write(binaryProtocol);
				} catch (TException e) {
					e.printStackTrace();
				}
			} catch (TException e) {
				e.printStackTrace();
			} 
		} 
		if(operationType.equalsIgnoreCase("delete")) {
			try {
				StatusReport statusReport = client.deleteFile(operationFileName);
				statusReport.write(binaryProtocol);
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
		}
		if(operationType.equalsIgnoreCase("write")) {
			RFile rFile = new RFile();
			rFile.meta = new RFileMetadata();
			String contents_File  = readFile(operationFileName);
			rFile.content=contents_File;
			rFile.meta.filename = operationFileName;
			try {
				StatusReport statusReport;
				System.out.println("A Request has been sent to write the file ---- "+ operationFileName +" ---  to Cordinator : Please Check JavaCordinator console.");
				statusReport = client.writeFile(rFile);
				statusReport.write(binaryProtocol);
			} catch (TException e) {
				e.printStackTrace();
			}
			
		
		}
		
	
	}
	
	public static void listFilesForFolder(File folder) {
	    for (File fileEntry : folder.listFiles()) {
	        if (fileEntry.isDirectory()) {
	            listFilesForFolder(fileEntry);
	        } else {
	        	fileInAFolder.add(fileEntry);
	        	//System.out.println("-----"+fileEntry.getName());
	        }
	    }
	}
	
	public static String readFile(String fileName) {
		File file = new File("./files");
		listFilesForFolder(file);
		StringBuilder stringBuilder = new StringBuilder();
		String lineFromFile = null;
		//System.out.println(fileInAFolder.toString());
		for(File singleFileFromList : fileInAFolder) {
			if(fileName.equals(singleFileFromList.getName())) {
				FileProcessor fileProcessor = new FileProcessor(singleFileFromList.getAbsolutePath(),singleFileFromList.getAbsolutePath());
				fileProcessor.openFile();
				while((lineFromFile = fileProcessor.readFromFile()) != null) {
					stringBuilder.append(lineFromFile);
				}
				return stringBuilder.toString();
			}
		}
		return "void";
	}
}
 