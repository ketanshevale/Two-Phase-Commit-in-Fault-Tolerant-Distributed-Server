JFLAGS = -g
JC = javac
JVM= java
JAR=".:/home/yaoliu/src_code/local/libthrift-1.0.0.jar:/home/yaoliu/src_code/local/slf4j-log4j12-1.5.8.jar:/home/yaoliu/src_code/local/slf4j-api-1.5.8.jar:/home/yaoliu/src_code/local/log4j-1.2.14.jar:sqlite-jdbc-3.8.11.2.jar" 
FILE=
.SUFFIXES: .java .class
.java.class:
	$(JC) -classpath $(JAR) $(JFLAGS) $*.java
CLASSES = \
	CoordinatorInfo.java \
	DbUtil.java \
	FileProcessor.java \
	FileProcessorSupport.java \
	FileStore_Cordinator.java  \
	FileStore_Participant.java \
	JavaClient.java \
	JavaCordinator.java \
	JavaCordinatorFileHandler.java \
	JavaParticipant.java \
	JavaParticipantFileHandler.java \
	LogInfoCordinator.java \
	LogInfoCordinatorDAO.java \
	LogInfoCordinatorDAOSupport.java \
	LogInfoParticipant.java \
	LogInfoParticipantDAO.java \
	LogInfoParticipantDAOSupport.java \
	RandomNumberGenerator.java \
	ReplicaInfo.java \
	RFile.java \
	RFileMetadata.java \
	SQLLiteJDBCCordinator.java \
	SQLLiteJDBCDatabase.java \
	SQLLiteJDBCParticpant.java \
	Status.java \
	StatusReport.java \
	StatusReport_ParticipantTPCMMessage.java \
	SystemException.java \


default: classes

classes: $(CLASSES:.java=.class)

run: $(MAIN).class
	$(JVM) -cp .:libthrift-0.9.1.jar:logback-access-1.1.2.jar:logback-classic-1.1.2.jar:logback-core-1.1.2.jar:slf4j-api-1.7.7.jar:sqlite-jdbc-3.8.11.2.jar  $(MAIN)

clean:
	$(RM) *.class

