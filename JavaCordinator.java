

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

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


public class JavaCordinator {
	
	public static FileStore_Cordinator.Processor<FileStore_Cordinator.Iface> processor_coorddinator = null;
	public static JavaCordinatorFileHandler cordinatorfileHandler = null;
	static List<ReplicaInfo> replicaInfoList = new ArrayList<ReplicaInfo>();
	private static LogInfoCordinatorDAO dao = new LogInfoCordinatorDAO();
	private static Map<Integer, CopyOnWriteArrayList<StatusReport_ParticipantTPCMMessage>> mapEntry = new ConcurrentHashMap<Integer,CopyOnWriteArrayList<StatusReport_ParticipantTPCMMessage>>();
	
	public static List<ReplicaInfo> readFromFile(FileProcessorSupport fileProcessorSupport) {

		String readLine;
		List<ReplicaInfo> listReplicaID = new ArrayList<ReplicaInfo>();
		ReplicaInfo replicaInfo = null;
		/**
		 * Open the file for operation
		 */
		fileProcessorSupport.openFile();
		while ((readLine = fileProcessorSupport.readFromFile()) != null) {
			if(readLine.isEmpty() || readLine.trim().equals("") || readLine.trim().equals("\n")) {
				
			} else {
				replicaInfo = new ReplicaInfo();
				String tokens[] = readLine.split("\\s+");
				replicaInfo.setName(tokens[0].trim().toLowerCase());
				replicaInfo.setIp(tokens[1].trim());
				replicaInfo.setPort(Integer.parseInt(tokens[2]));
				listReplicaID.add(replicaInfo);
			}
		}
		return listReplicaID;

	}
	
	static void performRecovery(String operation) {
		
		List<LogInfoCordinator> logInfoRecovery = new CopyOnWriteArrayList<LogInfoCordinator>();
		
		String status = "VOTE_REQUEST";
		RFile rFile = new RFile();
		RFileMetadata rFileMetadata = new RFileMetadata();
		
		logInfoRecovery = dao.findLogInfoByStatus(status);
		System.out.println("logInfoRecovery  ---- "+logInfoRecovery.toString());
		// Traverse through the list
		FileStore_Participant.Client client = null;
		for(LogInfoCordinator logInfoParticipant : logInfoRecovery) {
			StatusReport_ParticipantTPCMMessage singleTPCMMessage = new StatusReport_ParticipantTPCMMessage();
			CopyOnWriteArrayList<StatusReport_ParticipantTPCMMessage> single = new CopyOnWriteArrayList<StatusReport_ParticipantTPCMMessage>();
			for(ReplicaInfo replicaInfo : replicaInfoList){
				TTransport transport = new TSocket(replicaInfo.getIp(), replicaInfo.getPort());
				try {
					transport.open();
				} catch (TTransportException e) {
					System.err.println("Error while opening the transaction");
				}
				TProtocol protocol = new TBinaryProtocol(transport);
				client = new FileStore_Participant.Client(protocol);
				rFileMetadata.setMessage(logInfoParticipant.status);
				rFileMetadata.setFilename(logInfoParticipant.fileName);
				rFile.setContent(logInfoParticipant.getContent_File());
				rFile.setMeta(rFileMetadata);
				singleTPCMMessage.setTransaction_Id(logInfoParticipant.transaction_Info);
				singleTPCMMessage.setStatusReport_TPCMMessage("RECOVERY");
				StatusReport_ParticipantTPCMMessage statusReport;
				try {
					statusReport = client.writeFile(rFile, singleTPCMMessage);
					System.out.println("Return message from cordinator ["+statusReport.statusReport_TPCMMessage+"]");
					dao.updateStatus(logInfoParticipant.fileName, logInfoParticipant.transaction_Info, operation, statusReport.statusReport_TPCMMessage);
					single.add(statusReport);
				} catch (TException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			mapEntry.put(Integer.valueOf(logInfoParticipant.getTransaction_Info()), single);
			
		}
		if(rFile.meta != null ) {
			boolean commit_flag = false;
			int transID = 0;
			for (Entry<Integer, CopyOnWriteArrayList<StatusReport_ParticipantTPCMMessage>> entry : mapEntry.entrySet())
			{
				System.out.println("Recovery entry.Key : "+entry.getKey());
				System.out.println("Recovery entry.Value : "+entry.getValue().toString());
					for(StatusReport_ParticipantTPCMMessage singleStatus : entry.getValue()) {
						if((Integer.valueOf(singleStatus.getTransaction_Id()).equals(entry.getKey())) && singleStatus.getStatusReport_TPCMMessage().equals("VOTE_COMMIT")) {
							commit_flag = true;
							transID = singleStatus.getTransaction_Id();
						} else {
							commit_flag = false;
							transID = singleStatus.getTransaction_Id();
							//break;
						}
				}
			}
			System.out.println(commit_flag);
			if(commit_flag) {
				for(ReplicaInfo  replicaInfo : replicaInfoList) {
					System.out.println("replicaInfo.getIp()"+ replicaInfo.getIp()+"  ----- replicaInfo.getPort()  --- "+replicaInfo.getPort()+"   |");
					TTransport transport = new TSocket(replicaInfo.getIp(),replicaInfo.getPort());
					try {
						transport.open();
					} catch (TTransportException e) {
						System.err.println("Error while opening the transaction");
					}
					TProtocol protocol = new TBinaryProtocol(transport);
					FileStore_Participant.Client client1 = new  FileStore_Participant.Client(protocol) ;
					try {
						Thread.sleep(700);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					String transactionMessage = "GLOBAL_COMMIT";
					StatusReport_ParticipantTPCMMessage statusReport_ParticipantTPCMMessage = new StatusReport_ParticipantTPCMMessage();
					statusReport_ParticipantTPCMMessage.setStatusReport_TPCMMessage(transactionMessage);
					statusReport_ParticipantTPCMMessage.setTransaction_Id(transID);
					
					dao.updateStatus(rFile.getMeta().getFilename(), transID, operation, transactionMessage);
					try {
						client1.writeFile(rFile, statusReport_ParticipantTPCMMessage);
					} catch (TException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					//transport.close();
				}
			} else if(commit_flag == false) {
				System.out.println("Inside ABORT");
				for(ReplicaInfo  replicaInfo : replicaInfoList) {
					TTransport transport = new TSocket(replicaInfo.getIp(),replicaInfo.getPort());
					try {
						transport.open();
					} catch (TTransportException e) { 
						System.err.println("Error while opening the transaction");
					}
					TProtocol protocol = new TBinaryProtocol(transport);
					FileStore_Participant.Client client1 = new  FileStore_Participant.Client(protocol) ;
					
					String transactionMessage = "GLOBAL_ABORT";
					
					StatusReport_ParticipantTPCMMessage statusReport_ParticipantTPCMMessage = new StatusReport_ParticipantTPCMMessage();
					statusReport_ParticipantTPCMMessage.setStatusReport_TPCMMessage(transactionMessage);
					statusReport_ParticipantTPCMMessage.setTransaction_Id(transID);
					System.out.println(rFile.getMeta().getFilename());
					
					dao.updateStatus(rFile.getMeta().getFilename(), transID, operation, transactionMessage);
					try {
						client1.writeFile(rFile, statusReport_ParticipantTPCMMessage);
					} catch (TException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					//transport.close();
				}
		
		}
		// Send the response depending upon the recovery 
			}
	}
	
	public static void main(String[] args) {
	
		cordinatorfileHandler = new JavaCordinatorFileHandler();
		processor_coorddinator = new FileStore_Cordinator.Processor<FileStore_Cordinator.Iface>(cordinatorfileHandler);
		
		int portNumber = 0;
			
		if(args.length != 1) {
			System.err.println("Please provide the arguments properly JavaServer <portNumber>");
			System.exit(1);
		}
		portNumber = Integer.parseInt(args[0]);
		FileProcessorSupport fileProcessorSupport = new FileProcessor("replicas.txt","replicas.txt");
		replicaInfoList = readFromFile(fileProcessorSupport);
		System.out.println(replicaInfoList.toString());
		LogInfoCordinator logInfoCordinator = dao.getLatestTransactionInfo();
		JavaCordinatorFileHandler.transactionID = new AtomicInteger(logInfoCordinator.getTransaction_Info());
		JavaCordinatorFileHandler.listOfAll_Replicas = replicaInfoList;
		
		performRecovery("WRITE");
		performRecovery("DELETE");
				
		simple_coordinator(processor_coorddinator, portNumber);
		
	}
	public static void simple_coordinator(FileStore_Cordinator.Processor<FileStore_Cordinator.Iface> processor, int portNumber) {
	    try {
	      TServerTransport serverTransport = new TServerSocket(portNumber);
	      //TServer server = new TSimpleServer(new Args(serverTransport).processor(processor));

	      // Use this for a multithreaded server
	      TServer server = new TThreadPoolServer(new TThreadPoolServer.Args(serverTransport).processor(processor));
	      
	      System.out.println("Starting the simple Coordinator Server... "+portNumber);
	      server.serve();
	      serverTransport.close();
	    } catch (Exception e) {
	      e.printStackTrace();
	    }
	  }

}
