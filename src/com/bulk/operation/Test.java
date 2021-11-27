package com.bulk.operation;

import com.documentum.com.DfClientX;
import com.documentum.com.IDfClientX;
import com.documentum.fc.client.DfQuery;
import com.documentum.fc.client.IDfClient;
import com.documentum.fc.client.IDfCollection;
import com.documentum.fc.client.IDfQuery;
import com.documentum.fc.client.IDfSession;
import com.documentum.fc.client.IDfSessionManager;
import com.documentum.fc.client.IDfSysObject;
import com.documentum.fc.common.DfException;
import com.documentum.fc.common.DfId;
import com.documentum.fc.common.IDfLoginInfo;

public class Test {

	public static void main(String[] args) {
		try {
			 
			IDfClientX clientX =  new DfClientX();
			IDfClient client  = clientX.getLocalClient();
			IDfLoginInfo loginInfo = clientX.getLoginInfo();
			loginInfo.setUser("");
			loginInfo.setPassword("");
			loginInfo.setDomain(null);
			IDfSessionManager sMgr =  client.newSessionManager();
			sMgr.setIdentity("", loginInfo);
			IDfSession session = sMgr.newSession("");
			System.out.println("iniliatized connection");
			cancelCheckout(session);

		}catch (Exception e) {
			System.err.println("Failed to initialize"+e);
		}

	}
	static void checkID(String id) {
		System.out.println(new DfId(id).getTypePart());
		
	}
	static void query(String dql, IDfSession sn, String[] attributes) throws Exception{
		int limit = 10;
		IDfQuery q =  new DfQuery();
		q.setDQL(dql);
		IDfCollection col =  q.execute(sn,IDfQuery.DF_READ_QUERY);
		
		String[] attrNames  = new String[attributes.length+2];
		attrNames[0] = "r_object_id";
		attrNames[1] = "object_name";
		for(int i=2;i<attributes.length+2;i++) {
			attrNames[i] = attributes[i-2];
			
		}
		
		System.out.println("".join("  ", attrNames));		
		while(col.next()) {
			String out = "";
			for(String attr:attrNames) {
				out+= (11-limit)+".  "+col.getString(attr)+"  ";
			}
			System.out.println(out);
			
			if(--limit==0)break;
		}
		col.close();
		
	}
	static void cancelCheckout(IDfSession session) throws DfException {
		IDfSysObject obj1 =  (IDfSysObject) session.getObject(new DfId("090030398004ed3a"));
		IDfSysObject obj2 =  (IDfSysObject) session.getObject(new DfId("090030398004f586"));
		obj1.cancelCheckout();
		obj2.cancelCheckout();
		obj1.checkin(true, "CURRENT");
		obj2.checkin(true, "CURRENT");
	}

}
