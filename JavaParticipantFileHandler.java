
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Scanner;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.thrift.TException;

public class JavaParticipantFileHandler implements FileStore_Participant.Iface {

	private LogInfoParticipantDAO dao;
	public static AtomicInteger transactionID = new AtomicInteger(1);
	public static CoordinatorInfo cordinatorInfo = new CoordinatorInfo();
	public static Hashtable<String, RFile> fileNameandContents = null;

	/**
	 * A public constructor which initializes the DAO Object
	 */
	public JavaParticipantFileHandler() {
		dao = new LogInfoParticipantDAO();
		fileNameandContents = new Hashtable<String, RFile>();
	}

	@Override
	public StatusReport_ParticipantTPCMMessage writeFile(RFile rFile,StatusReport_ParticipantTPCMMessage statusReport_ParticipantTPCMMessage)
					throws SystemException, TException {
		
		// Check the conditions for Second write file Operation
		
		
		if(statusReport_ParticipantTPCMMessage.getStatusReport_TPCMMessage().equals("RECOVERY")) {
			StatusReport_ParticipantTPCMMessage status = new StatusReport_ParticipantTPCMMessage();
			LogInfoParticipant logInfoParticipant = dao.findLogInfoByFilenameandTransactionId(rFile.meta.filename, statusReport_ParticipantTPCMMessage.transaction_Id);
			System.out.println("A message has been prepared for "+logInfoParticipant.status+" and has been sent to the Cordinator  [Transaction ID: "+statusReport_ParticipantTPCMMessage.transaction_Id+"]");
			status.setTransaction_Id(logInfoParticipant.transaction_Info);
			status.setStatusReport_TPCMMessage(logInfoParticipant.status);
			return status;
		}
		if(statusReport_ParticipantTPCMMessage.getStatusReport_TPCMMessage().equals("GLOBAL_COMMIT")) {
			dao.updateStatus(rFile.getMeta().getFilename(), statusReport_ParticipantTPCMMessage.transaction_Id, "WRITE", statusReport_ParticipantTPCMMessage.statusReport_TPCMMessage);
			System.out.println("Performing Global Commit operations. There is no use in checking the file directory  [filename: "+rFile.getMeta().getFilename()+"]");
			try {
				
				// Update the database
				File file = new File("./files/writtenFilehandler/" + rFile.getMeta().filename);
				if (!file.exists()) {
					file.createNewFile();
				}

				FileWriter fw = new FileWriter(file.getAbsoluteFile(),true);
				BufferedWriter bw = new BufferedWriter(fw);
				bw.write(rFile.getContent());
				bw.close();
				fileNameandContents.put(rFile.getMeta().filename, rFile);
			} catch (IOException e) {
				SystemException systemException = new SystemException();
				systemException.setMessage("Error ocuurred while writing the file to the server side");
			}
			//rFile.setMeta(rFile.getMeta());
			
		}
		if(statusReport_ParticipantTPCMMessage.getStatusReport_TPCMMessage().equals("GLOBAL_ABORT")) {
			dao.updateStatus(rFile.getMeta().getFilename(), statusReport_ParticipantTPCMMessage.transaction_Id, "WRITE", statusReport_ParticipantTPCMMessage.statusReport_TPCMMessage);
			System.out.println("Performing Global Abort operations. There is no use in checking the file directory  [filename: "+rFile.getMeta().getFilename()+"]");	
		}
		
		if(statusReport_ParticipantTPCMMessage.statusReport_TPCMMessage.equals("VOTE_REQUEST")) {
			// LogInfoParticipant [operation_Type=null, fileName=null, status=null, content_File=null, transaction_Info=0]
				
			

			/*LogInfoParticipant check = new LogInfoParticipant();
			boolean secondCheck = false;
			synchronized (rFile.getMeta().getFilename()) {
				check = dao.findStatusForConcurrency("VOTE_COMMIT", "VOTE_ABORT","VOTE_REQUEST",rFile.getMeta().getFilename());
				System.out.println("Synch "+check.toString());
				
				if(check!=null) {
					if(check.getTransaction_Info() == 0) {
						if(check.getFileName() == null) {
							if(check.getOperation_Type() == null) {
								if(check.getStatus()==null) {
									if(check.getContent_File() == null) {
										secondCheck = true;
									}
								}
							}
						}
					}
				} 
				
				System.out.println("Inserting into the database "+secondCheck);
				if(secondCheck) {
					dao.insertIntoParticpantLog("WRITE", statusReport_ParticipantTPCMMessage.transaction_Id, rFile.meta.filename, "VOTE_REQUEST", rFile.content);
				} 
			}
			if(!secondCheck ) {
				if(check.getStatus()=="VOTE_COMMIT") {
					dao.insertIntoParticpantLog("WRITE", statusReport_ParticipantTPCMMessage.transaction_Id, rFile.meta.filename, "VOTE_ABORT", rFile.content);
					System.out.println("A message has been prepared for VOTE_ABORT and has been sent to the Cordinator  [Transaction ID: "+statusReport_ParticipantTPCMMessage.transaction_Id+"]");
					statusReport_ParticipantTPCMMessage.statusReport_TPCMMessage = "VOTE_ABORT";
					return statusReport_ParticipantTPCMMessage;
				}
			}
			
			*/
			Scanner userInput = new Scanner(System.in);
			int choice = 0;
			synchronized(this) {
				System.out.println("===================================================================================================");
				System.out.println("Perform the Write Operation for the Participant - [Transaction ID: "+statusReport_ParticipantTPCMMessage.transaction_Id+"]"
						+ "[ Filename: "+rFile.getMeta().filename+"]" +" [Transaction Message from Cordinator:  "+statusReport_ParticipantTPCMMessage.statusReport_TPCMMessage+"]");
				System.out.println("1. VOTE_COMMIT");
				System.out.println("2. VOTE_ABORT");
				System.out.println("3. VOTE_COMMIT AND CRASH");
				System.out.println("===================================================================================================");
				System.out.println("Please Enter you choice as 1 | 2 | 3 : ");
				choice = userInput.nextInt();
			}
			if(choice == 1) {
				//dao.insertIntoParticpantLog("WRITE", statusReport_ParticipantTPCMMessage.transaction_Id, rFile.meta.filename, "VOTE_COMMIT", rFile.content);
				dao.updateStatus( rFile.meta.filename,statusReport_ParticipantTPCMMessage.transaction_Id, "WRITE", "VOTE_COMMIT");
				System.out.println("A message has been prepared for VOTE_COMMIT and has been sent to the Cordinator  [Transaction ID: "+statusReport_ParticipantTPCMMessage.transaction_Id+"]");
				statusReport_ParticipantTPCMMessage.statusReport_TPCMMessage = "VOTE_COMMIT";
				return statusReport_ParticipantTPCMMessage;
			} 
			if(choice == 2) {
				//dao.insertIntoParticpantLog("WRITE", statusReport_ParticipantTPCMMessage.transaction_Id, rFile.meta.filename, "VOTE_ABORT", rFile.content);
				dao.updateStatus( rFile.meta.filename,statusReport_ParticipantTPCMMessage.transaction_Id, "WRITE", "VOTE_ABORT");
				System.out.println("A message has been prepared for VOTE_ABORT and has been sent to the Cordinator  [Transaction ID: "+statusReport_ParticipantTPCMMessage.transaction_Id+"]");
				statusReport_ParticipantTPCMMessage.statusReport_TPCMMessage = "VOTE_ABORT";
				return statusReport_ParticipantTPCMMessage;
		
			} 
			if(choice == 3) {
				
				
				// Code for the recover of the particpant
				//dao.insertIntoParticpantLog("WRITE", statusReport_ParticipantTPCMMessage.transaction_Id, rFile.meta.filename, "VOTE_COMMIT", rFile.content);
				dao.updateStatus( rFile.meta.filename,statusReport_ParticipantTPCMMessage.transaction_Id, "WRITE", "VOTE_COMMIT");
				System.out.println("VOTE COMMIT AND CRASH "+dao.findAllLogInfo());
				// Get the list according to the status as VOTE_COMMIT
				statusReport_ParticipantTPCMMessage.statusReport_TPCMMessage = "VOTE_COMMIT";
				
				Runnable simple = new Runnable() {
					public void run() {
						try {
							Thread.sleep(350);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						System.exit(0);
					}
				};
				new Thread(simple).start();
				return statusReport_ParticipantTPCMMessage;			 
			}
			
		
		}
			return statusReport_ParticipantTPCMMessage;
	}

	@Override
	public RFile readFile(String filename) throws SystemException, TException {
		System.out.println("Particpant has got the request for reading the file [filename : "+filename+"]");
		RFile localFileToreturn = null;
		boolean isFound = false;
		RFile localRead = fileNameandContents.get(filename);
		if (localRead == null) {
			SystemException exception = new SystemException();
			exception.setMessage("File not found.");
			throw exception;
		} else {
			isFound = true;
		}
		if (isFound) {
			localFileToreturn = localRead;
			return localFileToreturn;
		} else {
			SystemException exception = new SystemException();
			exception.setMessage("File not found because the owner name is different");
			throw exception;

		}
	}

	@Override
	public StatusReport_ParticipantTPCMMessage deleteFile(String filename,StatusReport_ParticipantTPCMMessage statusReport_ParticipantTPCMMessage)
					throws SystemException, TException {
		
		// Check the conditions for Second write file Operation
		
		
		if(statusReport_ParticipantTPCMMessage.getStatusReport_TPCMMessage().equals("RECOVERY")) {
			StatusReport_ParticipantTPCMMessage status = new StatusReport_ParticipantTPCMMessage();
			LogInfoParticipant logInfoParticipant = dao.findLogInfoByFilenameandTransactionId(filename, statusReport_ParticipantTPCMMessage.transaction_Id);
			System.out.println("A message has been prepared for "+logInfoParticipant.status+" and has been sent to the Cordinator  [Transaction ID: "+statusReport_ParticipantTPCMMessage.transaction_Id+"]");
			status.setTransaction_Id(logInfoParticipant.transaction_Info);
			 status.setStatusReport_TPCMMessage(logInfoParticipant.status);
			return status;
		}
		if(statusReport_ParticipantTPCMMessage.getStatusReport_TPCMMessage().equals("GLOBAL_COMMIT")) {
			dao.updateStatus(filename, statusReport_ParticipantTPCMMessage.transaction_Id, "DELETE", statusReport_ParticipantTPCMMessage.statusReport_TPCMMessage);
			System.out.println("Performing Global Commit operations. Please check the file directory ! [filename: "+filename+"]");
			// Update the database
			File file = new File("./files/writtenFilehandler/");
			
			if (file.isDirectory()) {
				File[] files = file.listFiles();
				for (File temp : files) {
					if (temp.getName().equalsIgnoreCase(filename)) {
						temp.delete();
					}
				}
			}
			
		}
		if(statusReport_ParticipantTPCMMessage.getStatusReport_TPCMMessage().equals("GLOBAL_ABORT")) {
			dao.updateStatus(filename, statusReport_ParticipantTPCMMessage.transaction_Id, "DELETE", statusReport_ParticipantTPCMMessage.statusReport_TPCMMessage);
			System.out.println("Performing Global Abort operations. There is no use in checking the file directory !  [filename: "+filename+"]");	
		}
		
		if(statusReport_ParticipantTPCMMessage.statusReport_TPCMMessage.equals("VOTE_REQUEST")) {	
			/*
			synchronized (filename) {
				LogInfoParticipant check = dao.findStatusForConcurrency("VOTE_COMMIT", "VOTE_ABORT","VOTE_REQUEST",filename);
				System.out.println("Synch "+check.toString());
				boolean secondCheck = false;
				if(check!=null) {
					if(check.getTransaction_Info() == 0) {
						if(check.getFileName() == null) {
							if(check.getOperation_Type() == null) {
								if(check.getStatus()==null) {
									if(check.getContent_File() == null) {
										secondCheck = true;
									}
								}
							}
						}
					}
				} 
				
				System.out.println("Inserting into the database "+secondCheck);
				if(secondCheck) {
					dao.insertIntoParticpantLog("DELETE", statusReport_ParticipantTPCMMessage.transaction_Id, filename, "VOTE_REQUEST", "EMPTY");
				} else if(!secondCheck ) {
					dao.insertIntoParticpantLog("DELETE", statusReport_ParticipantTPCMMessage.transaction_Id, filename, "VOTE_ABORT", "EMPTY");
					System.out.println("A message has been prepared for VOTE_ABORT and has been sent to the Cordinator  [Transaction ID: "+statusReport_ParticipantTPCMMessage.transaction_Id+"]");
					statusReport_ParticipantTPCMMessage.statusReport_TPCMMessage = "VOTE_ABORT";
					return statusReport_ParticipantTPCMMessage;
				}
			}
			
			*/ 
			Scanner userInput = new Scanner(System.in);
			int choice = 0;
			synchronized(this) {
				System.out.println("===================================================================================================");
				System.out.println("Perform the Write Operation for the Participant - [Transaction ID: "+statusReport_ParticipantTPCMMessage.transaction_Id+"]"
						+ "[ Filename: "+filename+"]" +" [Transaction Message from Cordinator:  "+statusReport_ParticipantTPCMMessage.statusReport_TPCMMessage+"]");
				System.out.println("1. VOTE_COMMIT");
				System.out.println("2. VOTE_ABORT");
				System.out.println("3. VOTE_COMMIT AND CRASH");
				System.out.println("===================================================================================================");
				System.out.println("Please Enter you choice as 1 | 2 | 3 : ");
				choice = userInput.nextInt();
			}
			if(choice == 1) {
				//dao.insertIntoParticpantLog("DELETE", statusReport_ParticipantTPCMMessage.transaction_Id, filename, "VOTE_COMMIT", "EMPTY");
				dao.updateStatus(filename, statusReport_ParticipantTPCMMessage.transaction_Id, "DELETE", "VOTE_COMMIT");
				System.out.println("A message has been prepared for VOTE_COMMIT and has been sent to the Cordinator  [Transaction ID: "+statusReport_ParticipantTPCMMessage.transaction_Id+"]");
				statusReport_ParticipantTPCMMessage.statusReport_TPCMMessage = "VOTE_COMMIT";
				return statusReport_ParticipantTPCMMessage;
			} 
			if(choice == 2) {
				//dao.insertIntoParticpantLog("DELETE", statusReport_ParticipantTPCMMessage.transaction_Id,filename, "VOTE_ABORT", "EMPTY");
				dao.updateStatus(filename, statusReport_ParticipantTPCMMessage.transaction_Id, "DELETE", "VOTE_ABORT");
				System.out.println("A message has been prepared for VOTE_ABORT and has been sent to the Cordinator  [Transaction ID: "+statusReport_ParticipantTPCMMessage.transaction_Id+"]");
				statusReport_ParticipantTPCMMessage.statusReport_TPCMMessage = "VOTE_ABORT";
				return statusReport_ParticipantTPCMMessage;
		
			} 
			if(choice == 3) {
				
				
				// Code for the recover of the particpant
				//dao.insertIntoParticpantLog("DELETE", statusReport_ParticipantTPCMMessage.transaction_Id, filename, "VOTE_COMMIT", "EMPTY");
				dao.updateStatus(filename, statusReport_ParticipantTPCMMessage.transaction_Id, "DELETE", "VOTE_COMMIT");
				System.out.println("VOTE COMMIT AND CRASH "+dao.findAllLogInfo());
				// Get the list according to the status as VOTE_COMMIT
				statusReport_ParticipantTPCMMessage.statusReport_TPCMMessage = "VOTE_COMMIT";
				
				Runnable simple = new Runnable() {
					public void run() {
						try {
							Thread.sleep(350);
						} catch (InterruptedException e) {
							e.printStackTrace();
						}
						System.exit(0);
					}
				};
				new Thread(simple).start();
				return statusReport_ParticipantTPCMMessage;			 
			}
			}
		
			return statusReport_ParticipantTPCMMessage;
	}
	
}
