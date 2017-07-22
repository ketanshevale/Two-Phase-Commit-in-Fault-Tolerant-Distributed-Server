
public class LogInfoParticipant {
	
	@Override
	public String toString() {
		return "LogInfoParticipant [operation_Type=" + operation_Type + ", fileName=" + fileName + ", status=" + status
				+ ", content_File=" + content_File + ", transaction_Info=" + transaction_Info + "]";
	}
	String operation_Type;
	String fileName;
	String status;
	String content_File;
	int transaction_Info;
	
	public int getTransaction_Info() {
		return transaction_Info;
	}
	public void setTransaction_Info(int transaction_Info) {
		this.transaction_Info = transaction_Info;
	}
	public String getOperation_Type() {
		return operation_Type;
	}
	public void setOperation_Type(String operation_Type) {
		this.operation_Type = operation_Type;
	}
	public String getFileName() {
		return fileName;
	}
	public void setFileName(String fileName) { 
		this.fileName = fileName;
	}
	public String getStatus() {
		return status;
	}
	public void setStatus(String status) {
		this.status = status;
	}
	public String getContent_File() {
		return content_File;
	}
	public void setContent_File(String content_File) {
		this.content_File = content_File;
	}
}
