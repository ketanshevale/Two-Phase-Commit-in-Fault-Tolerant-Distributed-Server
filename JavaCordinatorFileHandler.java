
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.TTransportException;

public class JavaCordinatorFileHandler implements FileStore_Cordinator.Iface {
	
	public static Map<Integer,CopyOnWriteArrayList<StatusReport_ParticipantTPCMMessage>> statusReport_Write = new ConcurrentHashMap<Integer,CopyOnWriteArrayList<StatusReport_ParticipantTPCMMessage>>();
	public static CopyOnWriteArrayList<StatusReport_ParticipantTPCMMessage> statusRepor_Write_List = new CopyOnWriteArrayList<StatusReport_ParticipantTPCMMessage>();
	public static Map<Integer,CopyOnWriteArrayList<StatusReport_ParticipantTPCMMessage>> statusReport_Delete = new ConcurrentHashMap<Integer,CopyOnWriteArrayList<StatusReport_ParticipantTPCMMessage>>();
	public static CopyOnWriteArrayList<StatusReport_ParticipantTPCMMessage> statusRepor_Delete_List = new CopyOnWriteArrayList<StatusReport_ParticipantTPCMMessage>();
	
	public static List<ReplicaInfo> listOfAll_Replicas = new CopyOnWriteArrayList<ReplicaInfo>();
	public static FileStore_Participant.Client client = null;
	public static AtomicInteger transactionID;
	private LogInfoCordinatorDAO dao;

	/**
	 * A public constructor which initializes the DAO Object
	 */
	public JavaCordinatorFileHandler() {
		dao = new LogInfoCordinatorDAO();
	}

	@Override
	public synchronized StatusReport writeFile(final RFile rFile) throws SystemException, TException {
		StatusReport statusReport = new StatusReport();
		int choice = 0;
		synchronized(this) {
			Scanner userInput = new Scanner(System.in);
			System.out.println("===================================================================================================");
			System.out.println("Send the Write Operation requests to other Cordinator for file - " + "[ Filename: "
					+ rFile.getMeta().filename + "]");
			System.out.println("1. VOTE_REQUEST");
			System.out.println("2. CRASH");
			System.out.println("===================================================================================================");
			System.out.println("Please Enter you choice as 1 | 2 : ");
			choice = userInput.nextInt();
		}
		transactionID.getAndIncrement();

		if (choice == 1) {
			dao.insertIntoCordinatorLog("WRITE", transactionID.get(), rFile.meta.filename, "VOTE_REQUEST",rFile.content);
			Thread[] threads = new Thread[listOfAll_Replicas.size()];

			for (int i = 0; i < threads.length; i++) {
					TTransport transport = new TSocket(listOfAll_Replicas.get(i).getIp(), listOfAll_Replicas.get(i).getPort());
					try {
						transport.open();
					} catch (TTransportException e) {
						System.err.println("Error while opening the transaction");
					}
					TProtocol protocol = new TBinaryProtocol(transport);
					client = new FileStore_Participant.Client(protocol);

					threads[i] = new Thread(new Runnable() {
						public void run() {
							String transactionMessage = "VOTE_REQUEST";
							StatusReport_ParticipantTPCMMessage statusReport_ParticipantTPCMMessage = perform(client, rFile, "WRITE", transactionMessage);
							statusRepor_Write_List.add(statusReport_ParticipantTPCMMessage);
						}
					},"Thread#"+i);
					new Thread(threads[i]).start();
					//transport.close();
					
			}
			
			for (int i = 0; i < threads.length; i++) {
				try {
					while(statusRepor_Write_List.size() != listOfAll_Replicas.size()) {
						threads[i].join();
						
					}
					
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
		}
		if(choice == 2) {
			System.out.println("Sending Vote Request and Crashing.");
			

			dao.insertIntoCordinatorLog("WRITE", transactionID.get(), rFile.meta.filename, "VOTE_REQUEST",rFile.content);
			Thread[] threads = new Thread[listOfAll_Replicas.size()];

			for (int i = 0; i < threads.length; i++) {
					TTransport transport = new TSocket(listOfAll_Replicas.get(i).getIp(), listOfAll_Replicas.get(i).getPort());
					try {
						transport.open();
					} catch (TTransportException e) {
						System.err.println("Error while opening the transaction");
					}
					TProtocol protocol = new TBinaryProtocol(transport);
					client = new FileStore_Participant.Client(protocol);

					threads[i] = new Thread(new Runnable() {
						public void run() {
							String transactionMessage = "VOTE_REQUEST";
							/*StatusReport_ParticipantTPCMMessage statusReport_ParticipantTPCMMessage = */perform(client, rFile, "WRITE", transactionMessage);
							//statusRepor_Write_List.add(statusReport_ParticipantTPCMMessage);
						}
					},"Thread#"+i);
					new Thread(threads[i]).start();
					//transport.close();
					
			}
			
			Runnable simple = new Runnable() {
				public void run() {
					try {
						Thread.sleep(200);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					System.exit(0);
				}
			};
			new Thread(simple).start();
			
					
		}
		/*ExecutorService es = Executors.newCachedThreadPool();
		for(int i=0;i<5;i++)
		    es.execute(new Runnable() {   your task  });
		es.shutdown();
		try {
			boolean finshed = es.awaitTermination(1, TimeUnit.MINUTES);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}*/
		
		System.out.println("Sending Cordinator statusRepor_Write_List Contents: " + statusRepor_Write_List.toString());
		statusReport_Write.put(transactionID.get(), statusRepor_Write_List);
		// Check the commit request form the replicas
		boolean commit_flag = false;
		for (Entry<Integer, CopyOnWriteArrayList<StatusReport_ParticipantTPCMMessage>> entry : statusReport_Write.entrySet())
		{
			System.out.println("entry.Key : "+entry.getKey());
			System.out.println("entry.Value : "+entry.getValue().toString());
			if(entry.getKey().equals(Integer.valueOf(transactionID.get()))) {
				for(StatusReport_ParticipantTPCMMessage singleStatus : entry.getValue()) {
					if((Integer.valueOf(singleStatus.getTransaction_Id()).equals(entry.getKey())) && singleStatus.getStatusReport_TPCMMessage().equals("VOTE_COMMIT")) {
						commit_flag = true;
					} else {
						commit_flag = false;
						break;
					}
				}
			}
		}
		
		// update the database with the contents of the committing
		System.out.println("commit_flag ----- "+commit_flag);
		if(commit_flag) {
			for(ReplicaInfo  replicaInfo : listOfAll_Replicas) {
				System.out.println("replicaInfo.getIp()"+ replicaInfo.getIp()+"  ----- replicaInfo.getPort()  --- "+replicaInfo.getPort()+"   |");
				TTransport transport = new TSocket(replicaInfo.getIp(),replicaInfo.getPort());
				try {
					transport.open();
				} catch (TTransportException e) {
					System.err.println("Error while opening the transaction");
				}
				TProtocol protocol = new TBinaryProtocol(transport);
				FileStore_Participant.Client client = new  FileStore_Participant.Client(protocol) ;
				try {
					Thread.sleep(700);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				String transactionMessage = "GLOBAL_COMMIT";
				dao.updateStatus(rFile.getMeta().getFilename(), transactionID.get(), "WRITE", transactionMessage);
				perform(client, rFile, "WRITE", transactionMessage);
				//transport.close();
			}
			statusReport.status=Status.SUCCESSFUL;
			statusReport.message = "File has been written on the participants successfully.";
			
		} 
		if(!commit_flag) {
			for(ReplicaInfo  replicaInfo : listOfAll_Replicas) {
				System.out.println(" ABORT replicaInfo.getIp()"+replicaInfo.getIp()+" ---- replicaInfo.getPort() ------  "+replicaInfo.getPort());
				
				TTransport transport = new TSocket(replicaInfo.getIp(),replicaInfo.getPort());
				try {
					transport.open();
				} catch (TTransportException e) {
					System.err.println("Error while opening the transaction");
				}
				TProtocol protocol = new TBinaryProtocol(transport);
				FileStore_Participant.Client client = new  FileStore_Participant.Client(protocol) ;
				try {
					Thread.sleep(700);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				String transactionMessage = "GLOBAL_ABORT";
				dao.updateStatus(rFile.getMeta().getFilename(), transactionID.get(), "WRITE", transactionMessage);
				perform(client, rFile, "WRITE", transactionMessage);
				//transport.close();
			}
			statusReport = new StatusReport();
			statusReport.status=Status.FAILED;
			statusReport.message = "File has been written on the participants unsuccessfully.";
			
		}
		
		return statusReport;
	}

	protected StatusReport_ParticipantTPCMMessage perform(FileStore_Participant.Client client, RFile rFile, String string, String transactionMessage) {
		StatusReport_ParticipantTPCMMessage statusReport_ParticipantTPCMMessage = new StatusReport_ParticipantTPCMMessage();
		// TODO Auto-generated method stub
		try {
			statusReport_ParticipantTPCMMessage.setTransaction_Id(transactionID.get());
			statusReport_ParticipantTPCMMessage.setStatusReport_TPCMMessage(transactionMessage);
			statusReport_ParticipantTPCMMessage = client.writeFile(rFile, statusReport_ParticipantTPCMMessage);
			System.out.println(statusReport_ParticipantTPCMMessage.toString());
			//statusReport_Write.put(transactionID.get(), statusReport_ParticipantTPCMMessage);
			//statusRepor_Write_List.add(statusReport_ParticipantTPCMMessage);
		} catch (TException e) {
			// TODO Auto-generated catch block
			System.err.println("One of the Replica Server is down ["+e.getMessage()+"]");
		}
		return statusReport_ParticipantTPCMMessage;
	}

	
	protected StatusReport_ParticipantTPCMMessage perform_Delete(FileStore_Participant.Client client, String filename, String string, String transactionMessage) {
		StatusReport_ParticipantTPCMMessage statusReport_ParticipantTPCMMessage = new StatusReport_ParticipantTPCMMessage();
		// TODO Auto-generated method stub
		try {
			statusReport_ParticipantTPCMMessage.setTransaction_Id(transactionID.get());
			statusReport_ParticipantTPCMMessage.setStatusReport_TPCMMessage(transactionMessage);
			statusReport_ParticipantTPCMMessage = client.deleteFile(filename, statusReport_ParticipantTPCMMessage);
			System.out.println(statusReport_ParticipantTPCMMessage.toString());
			//statusReport_Write.put(transactionID.get(), statusReport_ParticipantTPCMMessage);
			//statusRepor_Write_List.add(statusReport_ParticipantTPCMMessage);
		} catch (TException e) {
			// TODO Auto-generated catch block
			System.err.println("One of the Replica Server is down ["+e.getMessage()+"]");
		}
		return statusReport_ParticipantTPCMMessage;
	}

	@Override
	public StatusReport deleteFile(final String filename) throws SystemException, TException {
		
		
		StatusReport statusReport = new StatusReport();
		int choice = 0;
		synchronized(this) {
			Scanner userInput = new Scanner(System.in);
			System.out.println("===================================================================================================");
			System.out.println("Send the Delete Operation requests to other Cordinator for file - " + "[ Filename: "
					+ filename + "]");
			System.out.println("1. VOTE_REQUEST");
			System.out.println("2. CRASH");
			System.out.println("===================================================================================================");
			System.out.println("Please Enter you choice as 1 | 2 : ");
			choice = userInput.nextInt();
		}
		transactionID.getAndIncrement();

		if (choice == 1) {
			dao.insertIntoCordinatorLog("DELETE", transactionID.get(), filename, "VOTE_REQUEST","EMPTY");
			Thread[] threads = new Thread[listOfAll_Replicas.size()];

			for (int i = 0; i < threads.length; i++) {
					TTransport transport = new TSocket(listOfAll_Replicas.get(i).getIp(), listOfAll_Replicas.get(i).getPort());
					try {
						transport.open();
					} catch (TTransportException e) {
						System.err.println("Error while opening the transaction");
					}
					TProtocol protocol = new TBinaryProtocol(transport);
					client = new FileStore_Participant.Client(protocol);

					threads[i] = new Thread(new Runnable() {
						public void run() {
							String transactionMessage = "VOTE_REQUEST";
							StatusReport_ParticipantTPCMMessage statusReport_ParticipantTPCMMessage = perform_Delete(client, filename, "DELETE", transactionMessage);
							statusRepor_Delete_List.add(statusReport_ParticipantTPCMMessage);
						}
					},"Thread#"+i);
					new Thread(threads[i]).start();
					//transport.close();
					
			}
			
			for (int i = 0; i < threads.length; i++) {
				try {
					while(statusRepor_Delete_List.size() != listOfAll_Replicas.size()) {
						threads[i].join();
						
					}
					
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			
		}
		if(choice == 2) {
			System.out.println("Sending Vote Request and Crashing.");
			

			dao.insertIntoCordinatorLog("DELETE", transactionID.get(),filename, "VOTE_REQUEST","EMPTY");
			Thread[] threads = new Thread[listOfAll_Replicas.size()];

			for (int i = 0; i < threads.length; i++) {
					TTransport transport = new TSocket(listOfAll_Replicas.get(i).getIp(), listOfAll_Replicas.get(i).getPort());
					try {
						transport.open();
					} catch (TTransportException e) {
						System.err.println("Error while opening the transaction");
					}
					TProtocol protocol = new TBinaryProtocol(transport);
					client = new FileStore_Participant.Client(protocol);

					threads[i] = new Thread(new Runnable() {
						public void run() {
							String transactionMessage = "VOTE_REQUEST";
							/*StatusReport_ParticipantTPCMMessage statusReport_ParticipantTPCMMessage = */perform_Delete(client, filename, "DELETE", transactionMessage);
							//statusRepor_Write_List.add(statusReport_ParticipantTPCMMessage);
						}
					},"Thread#"+i);
					new Thread(threads[i]).start();
					//transport.close();
					
			}
			
			Runnable simple = new Runnable() {
				public void run() {
					try {
						Thread.sleep(200);
					} catch (InterruptedException e) {
						e.printStackTrace();
					}
					System.exit(0);
				}
			};
			new Thread(simple).start();
			
					
		}
		/*ExecutorService es = Executors.newCachedThreadPool();
		for(int i=0;i<5;i++)
		    es.execute(new Runnable() {   your task  });
		es.shutdown();
		try {
			boolean finshed = es.awaitTermination(1, TimeUnit.MINUTES);
		} catch (InterruptedException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}*/
		
		System.out.println("Sending Cordinator statusRepor_Delete_List Contents: " + statusRepor_Delete_List.toString());
		statusReport_Delete.put(transactionID.get(), statusRepor_Delete_List);
		// Check the commit request form the replicas
		boolean commit_flag = false;
		for (Entry<Integer, CopyOnWriteArrayList<StatusReport_ParticipantTPCMMessage>> entry : statusReport_Delete.entrySet())
		{
			System.out.println("entry.Key : "+entry.getKey());
			System.out.println("entry.Value : "+entry.getValue().toString());
			if(entry.getKey().equals(Integer.valueOf(transactionID.get()))) {
				for(StatusReport_ParticipantTPCMMessage singleStatus : entry.getValue()) {
					if((Integer.valueOf(singleStatus.getTransaction_Id()).equals(entry.getKey())) && singleStatus.getStatusReport_TPCMMessage().equals("VOTE_COMMIT")) {
						commit_flag = true;
					} else {
						commit_flag = false;
						break;
					}
				}
			}
		}
		
		// update the database with the contents of the committing
		System.out.println("commit_flag ----- "+commit_flag);
		if(commit_flag) {
			for(ReplicaInfo  replicaInfo : listOfAll_Replicas) {
				System.out.println("replicaInfo.getIp()"+ replicaInfo.getIp()+"  ----- replicaInfo.getPort()  --- "+replicaInfo.getPort()+"   |");
				TTransport transport = new TSocket(replicaInfo.getIp(),replicaInfo.getPort());
				try {
					transport.open();
				} catch (TTransportException e) {
					System.err.println("Error while opening the transaction");
				}
				TProtocol protocol = new TBinaryProtocol(transport);
				FileStore_Participant.Client client = new  FileStore_Participant.Client(protocol) ;
				try {
					Thread.sleep(700);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				String transactionMessage = "GLOBAL_COMMIT";
				dao.updateStatus(filename, transactionID.get(), "DELETE", transactionMessage);
				perform_Delete(client, filename, "DELETE", transactionMessage);
				//transport.close();
			}
			statusReport.status=Status.SUCCESSFUL;
			statusReport.message = "File has been deleted on the participants successfully.";
			
		} 
		if(!commit_flag) {
			for(ReplicaInfo  replicaInfo : listOfAll_Replicas) {
				System.out.println(" ABORT replicaInfo.getIp()"+replicaInfo.getIp()+" ---- replicaInfo.getPort() ------  "+replicaInfo.getPort());
				
				TTransport transport = new TSocket(replicaInfo.getIp(),replicaInfo.getPort());
				try {
					transport.open();
				} catch (TTransportException e) {
					System.err.println("Error while opening the transaction");
				}
				TProtocol protocol = new TBinaryProtocol(transport);
				FileStore_Participant.Client client = new  FileStore_Participant.Client(protocol) ;
				try {
					Thread.sleep(700);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				String transactionMessage = "GLOBAL_ABORT";
				dao.updateStatus(filename, transactionID.get(), "DELETE", transactionMessage);
				perform_Delete(client, filename, "DELETE", transactionMessage);
				//transport.close();
			}
			statusReport = new StatusReport();
			statusReport.status=Status.FAILED;
			statusReport.message = "File has been deleted on the participants unsuccessfully.";
			
		}
		
		return statusReport;
	}

	@Override
	public RFile readFile(String filename) throws SystemException, TException {
		// TODO Auto-generated method stub
		RFile rFile = new RFile();
			int randIndex = RandomNumberGenerator.randInt(1, listOfAll_Replicas.size()-1);
			TTransport transport = new TSocket(listOfAll_Replicas.get(randIndex).getIp(),listOfAll_Replicas.get(randIndex).getPort());
			try {
				transport.open();
			} catch (TTransportException e) {
				randIndex = 0;		
				System.err.println("Error while opening the transaction Trying for Other Replicas "+listOfAll_Replicas.get(randIndex).getIp()+" "+listOfAll_Replicas.get(randIndex).getPort());
				TTransport transport1 = new TSocket(listOfAll_Replicas.get(randIndex).getIp(),listOfAll_Replicas.get(randIndex).getPort());
				transport1.open();
				TProtocol protocol = new TBinaryProtocol(transport1);
				FileStore_Participant.Client client = new  FileStore_Participant.Client(protocol) ;
				rFile = client.readFile(filename);
				return rFile;
			}
			TProtocol protocol = new TBinaryProtocol(transport);
			FileStore_Participant.Client client = new  FileStore_Participant.Client(protocol) ;
			rFile = client.readFile(filename);
		return rFile;
	}

	@Override
	public StatusReport can_Commit(String filename,StatusReport_ParticipantTPCMMessage statusReport_ParticipantTPCMMessage)
					throws SystemException, TException {
		LogInfoCordinator logInfoCordinator= dao.findLogInfoByFilenameandTransactionId(filename, statusReport_ParticipantTPCMMessage.getTransaction_Id());
		System.out.println("After every call the result is "+logInfoCordinator.toString());
		StatusReport statusReport = new StatusReport();
		statusReport.setMessage(logInfoCordinator.getStatus());
		statusReport.setStatus(Status.SUCCESSFUL);
		return statusReport;
	}

}
