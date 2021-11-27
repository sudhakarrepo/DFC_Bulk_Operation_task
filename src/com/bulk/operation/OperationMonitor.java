package com.bulk.operation;

import com.documentum.fc.common.DfException;
import com.documentum.operations.DfOperationMonitor;
import com.documentum.operations.IDfOperation;
import com.documentum.operations.IDfOperationError;
import com.documentum.operations.IDfOperationMonitor;
import com.documentum.operations.IDfOperationNode;
import com.documentum.operations.IDfOperationStep;

public class OperationMonitor extends DfOperationMonitor implements IDfOperationMonitor{

	@Override
	public int progressReport(IDfOperation op, int percent, IDfOperationStep st, int stepPercent, IDfOperationNode opNode)
			throws DfException {
		System.out.println("processing percent:"+percent+" step:"+st.getName()+" step-percent:"+stepPercent+" object-type:"+opNode.getPersistentProperties().getString("r_object_type")+" object-name:"+opNode.getPersistentProperties().getString("object_name"));
		return DfOperationMonitor.CONTINUE;
	}

	@Override
	public int getYesNoAnswer(IDfOperationError err) throws DfException {
		System.out.println("Error code:"+err.getErrorCode()+" message:"+ err.getMessage()+" operation is aborting");
		return DfOperationMonitor.ABORT;
	}

	 

	@Override
	public int reportError(IDfOperationError err) throws DfException {
		System.out.println("ERROR code:"+err.getErrorCode()+" message:"+err.getMessage()+" operation is aborting");
		return DfOperationMonitor.ABORT;
	}

}
