package com.bulk.operation;

import java.io.File;

import com.documentum.com.DfClientX;
import com.documentum.com.IDfClientX;
import com.documentum.fc.client.IDfClient;
import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfFolder;
import com.documentum.fc.client.IDfQuery;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfSessionManager;
import com.documentum.fc.client.IDfSysObject;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfId;
import com.documentum.fc.common.IDfList;
import com.documentum.fc.common.IDfLoginInfo;
import com.documentum.operations.IDfCheckinOperation;
import com.documentum.operations.IDfCheckoutOperation;
import com.documentum.operations.IDfExportOperation;
import com.documentum.operations.IDfImportOperation;
import com.documentum.operations.IDfOperationError;

public class BulkOperation {
	private IDfClientX clientX     = null;
	private IDfClient  client      = null;
	private IDfSessionManager sMgr = null;

	public BulkOperation() {
		this.init();
	}
	public static void start() throws Exception {
		BulkOperation bulkOpt = new BulkOperation();
		IDfSession session = bulkOpt.getSession("");
		System.out.println(session);
		bulkOpt.bulkCheckout(session, "/cabinet_name" );
		bulkOpt.bulkCheckin(session, "/cabinet_name", IDfCheckinOperation.SAME_VERSION);
		bulkOpt.bulkExport(session,"/cabinet_name", "./exports");
		bulkOpt.bulkImport(session, "./imports", "/cabinet_name");
		bulkOpt.releaseSession(session);
	}
	private void init() {	
		try {
			this.clientX =  new DfClientX();
			this.client  = clientX.getLocalClient();
			IDfLoginInfo loginInfo = clientX.getLoginInfo();
			loginInfo.setUser("");//<<--- must add cred's to connect
			loginInfo.setPassword("");
			loginInfo.setDomain(null);
			this.sMgr =  client.newSessionManager();
			this.sMgr.setIdentity("", loginInfo);
			System.out.println("iniliatized connection");
		}catch (Exception e) {
			System.err.println("Failed to initialize");
		}
	}
	private IDfSession getSession(String reposName) {
		try {
			return sMgr.newSession(reposName);
			
		}catch(Exception e) {
			return null;
		}
	}
	private void releaseSession(IDfSession session) {
		if(session!=null) {
			try {
				session.getSessionManager().release(session);
			}catch(Exception e) {}
		}
	}
	private void bulkImport( IDfSession session,String srcFileOrDirPath, String dstfolderPath ) throws Exception {
		if(session!=null && new File(srcFileOrDirPath).exists() ) {
			IDfFolder dstFolderObj = (IDfFolder) session.getObjectByPath(dstfolderPath);
			if(dstFolderObj==null) {
				throw new Exception("Error unable to find the destination folder path");
			}
			IDfImportOperation imOpt =  this.clientX.getImportOperation();
			imOpt.add(srcFileOrDirPath);
			imOpt.setSession(session);
			imOpt.setDestinationFolderId(dstFolderObj.getId("r_object_id"));
			imOpt.setKeepLocalFile(true);
			imOpt.setOperationMonitor(new OperationMonitor());
			boolean isSuccess =  imOpt.execute();
			if(isSuccess) {
				System.out.println("successfully imported  to ["+dstfolderPath+"]...");
			}
		}else {
			System.err.println("Error session is null OR src path ["+srcFileOrDirPath+"] is not a valid path");
		}
	}
	private void bulkExport(IDfSession session,String srcFolderPath, String dstLocalPath) throws Exception{
		if(session!=null && new File(dstLocalPath).isDirectory()) {
			IDfSysObject sysObj = (IDfSysObject) session.getObjectByPath(srcFolderPath);
			if(sysObj==null) {
				throw new Exception("Error invalid src folder path ["+srcFolderPath+"]");
			}
			IDfExportOperation exportOpt = this.clientX.getExportOperation();
			exportOpt.add(sysObj);
			exportOpt.setSession(session);
			exportOpt.setDestinationDirectory(dstLocalPath);
			exportOpt.setOperationMonitor(new OperationMonitor());
			boolean isSuccess =  exportOpt.execute();
			if(isSuccess) {
				System.out.println("successfully Exported "+(new File(srcFolderPath).isFile() ? "File" : "all files from dir")+
						"["+srcFolderPath+"]  to ["+dstLocalPath+"]...");
			}
		}else {
			System.err.println("session is not created OR invalid destination directory ["+dstLocalPath+"]");
		}
	}
	private void bulkCheckout(IDfSession session, String folderPath) throws Exception{
		if(session!=null) {
			IDfFolder folderObj =  (IDfFolder) session.getObjectByPath(folderPath);
			if(folderObj==null) throw new Exception("Error unable to find folder path ["+folderPath+"]");
			IDfCheckoutOperation checkoutOpt =  this.clientX.getCheckoutOperation();
			
			if(addCheckoutNodes(session, checkoutOpt, folderObj.getString("r_object_id"))) {
				checkoutOpt.setSession(session);
				checkoutOpt.setDownloadContent(false);
				checkoutOpt.setOperationMonitor(new OperationMonitor());
				boolean isSuccess = checkoutOpt.execute();
				
				if(isSuccess) {
					System.out.println("successfully Checkedout's all the file in dir["+folderPath+"]");
				}else {
					IDfList errors = checkoutOpt.getErrors();
					for(int i=0; i<errors.getCount();i++) {
						IDfOperationError error = (IDfOperationError) errors.get(i);
						System.err.println("Error code:"+error.getErrorCode()+" message:"+error.getMessage());
					}
				}
			}else {
				System.err.println("Error no node's are added to checkout operation");
			}
			
		}else {
			System.err.println("session is not created");
		}
	}
	private boolean addCheckoutNodes(IDfSession session,IDfCheckoutOperation opt ,String folderId) throws DfException {
		boolean isEmptyNode = false;
		IDfQuery queryObj =  this.clientX.getQuery();
		queryObj.setDQL( ("select r_object_id from dm_sysobject "
			+ "where  (any  i_folder_id = 'folderId' OR i_cabinet_id='folderId')"
			+ " AND NOT (r_object_id like '0b%' OR  r_object_id  like '0c%');").replaceAll("folderId", folderId) );
		IDfCollection col = null;
		
		try {
			col =  queryObj.execute(session, IDfQuery.DF_READ_QUERY);
			
			while(col.next()) {
				opt.add(session.getObject(new DfId(col.getString("r_object_id"))));
				isEmptyNode = true;
				System.out.println("added node["+col.getString("r_object_id")+"] to checkout operation "); // DEBUGG
			}
			
		}catch(DfException e) {System.err.println("Error failed to execute query OR unable to get object by id");} 
		finally {
			if(col!=null && col.getState()!= IDfCollection.DF_CLOSED_STATE) {
				col.close();
			}
		}
	
		return isEmptyNode;
	}
	private void bulkCheckin(IDfSession session, String folderPath, int checkinVersion) throws Exception{
		if(session!=null) {
			IDfFolder folderObj =  (IDfFolder) session.getObjectByPath(folderPath);
			if(folderObj==null) throw new Exception("Error unable to find folder path ["+folderPath+"]");
			IDfCheckinOperation checkinOpt =  this.clientX.getCheckinOperation();
			if(addCheckinNodes(session, checkinOpt, folderObj.getString("r_object_id"))) {
				checkinOpt.setSession(session);
				checkinOpt.setOperationMonitor(new OperationMonitor());
				checkinOpt.setCheckinVersion(checkinVersion);
				checkinOpt.setKeepLocalFile(false);
				boolean isSuccess = checkinOpt.execute();
				if(isSuccess) {
					System.out.println("successfully Checkedin's all the file in dir["+folderPath+"] as "+getVersionName(checkinVersion));
				}else {
					IDfList errors = checkinOpt.getErrors();
					for(int i=0; i<errors.getCount();i++) {
						IDfOperationError error = (IDfOperationError) errors.get(i);
						System.err.println("Error code:"+error.getErrorCode()+" message:"+error.getMessage());
					}
				}
			}else {
				System.err.println("Error no node's are added to checkin operation");
			}
			
		}
	}
	private boolean addCheckinNodes(IDfSession session,IDfCheckinOperation opt ,String folderId) throws DfException {
		boolean isEmptyNode = false;
		IDfQuery queryObj =  this.clientX.getQuery();
		queryObj.setDQL( ("select r_object_id from dm_sysobject "
			+ "where  (any  i_folder_id = 'folderId' OR i_cabinet_id='folderId')"
			+ " AND NOT (r_object_id like '0b%' OR  r_object_id  like '0c%');").replaceAll("folderId", folderId) );
		IDfCollection col = null;
		
		try {
			col =  queryObj.execute(session, IDfQuery.DF_READ_QUERY);
			
			while(col.next()) {
				opt.add(session.getObject(new DfId(col.getString("r_object_id"))));
				isEmptyNode = true;
				System.out.println("added node["+col.getString("r_object_id")+"] to checkin operation "); // DEBUGG
			}
			
			
		}catch(DfException e) {System.err.println("Error failed to execute query OR unable to get object by id");} 
		finally {
			if(col!=null && col.getState()!= IDfCollection.DF_CLOSED_STATE) {
				col.close();
			}
		}
	
		return isEmptyNode;
	}
	private String getVersionName(int checkinVersion) {
		String versionName = "unkown";
		switch (checkinVersion) {
		case IDfCheckinOperation.BRANCH_VERSION:
			versionName = "branch version";
			break;
		case IDfCheckinOperation.NEXT_MAJOR:
			versionName = "next major version";
			break;
		case IDfCheckinOperation.NEXT_MINOR:
			versionName = "next minor version";
			break;
		case IDfCheckinOperation.SAME_VERSION:
			versionName = "same version";
			break;	
		}
		return versionName;
	}

}
