

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TServerTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;


public class JavaParticipant {
	
	public static FileStore_Participant.Processor<FileStore_Participant.Iface> processor_participant = null;
	public static JavaParticipantFileHandler participantfileHandler = null;
	private static LogInfoParticipantDAO dao = new LogInfoParticipantDAO();
	public static List<File> fileInAFolder = new ArrayList<File>();

	public static CoordinatorInfo readFromFile(FileProcessorSupport fileProcessorSupport) {

		String readLine;
		CoordinatorInfo coordinatorInfo = null;
		/**
		 * Open the file for operation
		 */
		fileProcessorSupport.openFile();
		while ((readLine = fileProcessorSupport.readFromFile()) != null) {
			if(readLine.isEmpty() || readLine.trim().equals("") || readLine.trim().equals("\n")) {
				
			} else {
				coordinatorInfo = new CoordinatorInfo();
				String tokens[] = readLine.split("\\s+");
				coordinatorInfo.setName(tokens[0].trim().toLowerCase());
				coordinatorInfo.setIp(tokens[1].trim());
				coordinatorInfo.setPort(Integer.parseInt(tokens[2]));
			}
		}
		return coordinatorInfo;

	}

	static void performRecovery_Write(CoordinatorInfo coordinatorInfo) {
		List<LogInfoParticipant> logInfoRecovery = new CopyOnWriteArrayList<LogInfoParticipant>();
		String status = "VOTE_COMMIT";
		
		logInfoRecovery = dao.findLogInfoByStatusandOperatioType(status, "WRITE");
		System.out.println("logInfoRecovery  ---- "+logInfoRecovery.toString());
		// Traverse through the list
		FileStore_Cordinator.Client client = null;
		for(LogInfoParticipant logInfoParticipant : logInfoRecovery) {
			StatusReport_ParticipantTPCMMessage singleTPCMMessage = new StatusReport_ParticipantTPCMMessage();
			TTransport transport = new TSocket(coordinatorInfo.getIp(), coordinatorInfo.getPort());
			try {
				transport.open();
			} catch (TTransportException e) {
				System.err.println("Error while opening the transaction");
			}
			TProtocol protocol = new TBinaryProtocol(transport);
			client = new FileStore_Cordinator.Client(protocol);
			singleTPCMMessage.setTransaction_Id(logInfoParticipant.transaction_Info);
			singleTPCMMessage.setStatusReport_TPCMMessage(logInfoParticipant.status);
			StatusReport statusReport;
			try {
				statusReport = client.can_Commit(logInfoParticipant.fileName, singleTPCMMessage);
				System.out.println("Return message from cordinator ["+statusReport.message+"]");
				//dao.updateStatus(logInfoParticipant.fileName, logInfoParticipant.transaction_Info, "WRITE", statusReport.message);
				// String filename, int transaction, String opertion, String status
				//dao.updateStatus(statusReport.message, logInfoParticipant.transaction_Info, "WRITE", logInfoParticipant.fileName);
				dao.updateStatus(logInfoParticipant.fileName, logInfoParticipant.transaction_Info, "WRITE", statusReport.message);
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			// write the file and the hash map 
			try {
				File file = new File("./files/writtenFilehandler/" + logInfoParticipant.getFileName());
				if (!file.exists()) {
					file.createNewFile();
				}

				FileWriter fw = new FileWriter(file.getAbsoluteFile(),true);
				BufferedWriter bw = new BufferedWriter(fw);
				bw.write(logInfoParticipant.getContent_File());
				bw.close();
			}
			catch(IOException ioException) {
				ioException.printStackTrace();
			}
			RFile rfile = new RFile();
			RFileMetadata rFileMetadata = new RFileMetadata();
			rFileMetadata.setFilename(logInfoParticipant.fileName);
			rFileMetadata.setMessage(logInfoParticipant.status);
			rfile.setMeta(rFileMetadata);
			rfile.setContent(logInfoParticipant.content_File);
			//JavaParticipantFileHandler.fileNameandContents.put(logInfoParticipant.getFileName(), rfile);
		}

	}
	static void performRecovery_Delete(CoordinatorInfo coordinatorInfo) {
		List<LogInfoParticipant> logInfoRecovery = new CopyOnWriteArrayList<LogInfoParticipant>();
		String status = "VOTE_COMMIT";
		
		logInfoRecovery = dao.findLogInfoByStatusandOperatioType(status, "DELETE");
		System.out.println("logInfoRecovery  ---- "+logInfoRecovery.toString());
		// Traverse through the list
		FileStore_Cordinator.Client client = null;
		for(LogInfoParticipant logInfoParticipant : logInfoRecovery) {
			StatusReport_ParticipantTPCMMessage singleTPCMMessage = new StatusReport_ParticipantTPCMMessage();
			TTransport transport = new TSocket(coordinatorInfo.getIp(), coordinatorInfo.getPort());
			try {
				transport.open();
			} catch (TTransportException e) {
				System.err.println("Error while opening the transaction");
			}
			TProtocol protocol = new TBinaryProtocol(transport);
			client = new FileStore_Cordinator.Client(protocol);
			singleTPCMMessage.setTransaction_Id(logInfoParticipant.transaction_Info);
			singleTPCMMessage.setStatusReport_TPCMMessage(logInfoParticipant.status);
			StatusReport statusReport;
			try {
				statusReport = client.can_Commit(logInfoParticipant.fileName, singleTPCMMessage);
				System.out.println("Return message from cordinator ["+statusReport.message+"]");
				//dao.updateStatus(statusReport.message, logInfoParticipant.transaction_Info, "DELETE", logInfoParticipant.fileName);
				//dao.updateStatus(logInfoParticipant.fileName, logInfoParticipant.transaction_Info, "DELETE", statusReport.message);
				// String filename, int transaction, String opertion, String status
				//dao.updateStatus(statusReport.message, logInfoParticipant.transaction_Info, "WRITE", logInfoParticipant.fileName);
				dao.updateStatus(logInfoParticipant.fileName, logInfoParticipant.transaction_Info, "DELETE", statusReport.message);
		
			} catch (TException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			// write the file and the hash map 
			File file = new File("./files/writtenFilehandler/");
			
			if (file.isDirectory()) {
				File[] files = file.listFiles();
				for (File temp : files) {
					if (temp.getName().equalsIgnoreCase(logInfoParticipant.fileName)) {
						temp.delete();
					}
				}
			}
			RFile rfile = new RFile();
			RFileMetadata rFileMetadata = new RFileMetadata();
			rFileMetadata.setFilename(logInfoParticipant.fileName);
			rFileMetadata.setMessage(logInfoParticipant.status);
			rfile.setMeta(rFileMetadata);
			rfile.setContent(logInfoParticipant.content_File);
			//JavaParticipantFileHandler.fileNameandContents.put(logInfoParticipant.getFileName(), rfile);
		}

	}
	
	public static void main(String[] args) {
	
		participantfileHandler = new JavaParticipantFileHandler();
		processor_participant = new FileStore_Participant.Processor<FileStore_Participant.Iface>(participantfileHandler);
		CoordinatorInfo coordinatorInfo = new CoordinatorInfo();
		
		int portNumber = 0;
		String replicaDetails = null;	
		if(args.length != 2) {
			System.err.println("Please provide the arguments properly JavaServer <portNumber> <participantName>");
			System.exit(1);
		}
		portNumber = Integer.parseInt(args[0]);
		replicaDetails = args[1];
		FileProcessorSupport fileProcessorSupport = new FileProcessor("cordinator.txt","cordinator.txt");
		
		coordinatorInfo = readFromFile(fileProcessorSupport);
		System.out.println(coordinatorInfo.toString());
		JavaParticipantFileHandler.cordinatorInfo = coordinatorInfo;
		
		performRecovery_Delete(coordinatorInfo);
		performRecovery_Write(coordinatorInfo);
		
		//List<LogInfoParticipant>  stableList = new CopyOnWriteArrayList<LogInfoParticipant>();
		File file = new File("./files/writtenFilehandler/");
		
		if (file.isDirectory()) {
			File[] files = file.listFiles();
			for (File temp : files) {
				RFile rFile = new RFile();
				RFileMetadata rFileMetadata = new RFileMetadata();
				rFileMetadata.setFilename(temp.getName());
				//rFileMetadata.setMessage(logInfoParticipant.getStatus());
				rFile.setMeta(rFileMetadata);
				rFile.setContent(readFile(temp.getName()));
				System.out.println(rFile.getContent());
				JavaParticipantFileHandler.fileNameandContents.put(temp.getName(), rFile);
			}
		}
		simple_Participant(processor_participant, portNumber,replicaDetails);
	}
	public static void simple_Participant(FileStore_Participant.Processor<FileStore_Participant.Iface> processor, int portNumber,String replicaDetails) {
	    try {
	      TServerTransport serverTransport = new TServerSocket(portNumber);
	    
	      // Use this for a multithreaded server
	      TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));

	      System.out.println("Starting the simple Participant Server... portNumber : "+portNumber+" replicaDetails : "+replicaDetails);
	      server.serve();
	      serverTransport.close();
	    } catch (Exception e) {
	      e.printStackTrace();
	    }
	  }
	
	public static String readFile(String fileName) {
		File file = new File("./files/writtenFileHandler/"+fileName);
		StringBuilder stringBuilder = new StringBuilder();
		String lineFromFile = null;
		//System.out.println(fileInAFolder.toString());
				FileProcessor fileProcessor = new FileProcessor(file.getAbsolutePath(),file.getAbsolutePath());
				fileProcessor.openFile();
				while((lineFromFile = fileProcessor.readFromFile()) != null) {
					stringBuilder.append(lineFromFile);
				return stringBuilder.toString();
		}
		return "void";
	}
}
