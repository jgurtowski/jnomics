package edu.cshl.schatz.jnomics.tools;


import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;

import us.kbase.auth.AuthService;
import us.kbase.auth.AuthToken;
import us.kbase.auth.TokenFormatException;
import us.kbase.common.service.JsonClientException;
import us.kbase.common.service.Tuple12;
import us.kbase.common.service.Tuple8;
import us.kbase.common.service.UObject;
import us.kbase.common.service.UnauthorizedException;
import us.kbase.workspace.CreateWorkspaceParams;
import us.kbase.workspace.GetObjectOutput;
import us.kbase.workspace.GetObjectParams;
import us.kbase.workspace.ListWorkspaceObjectsParams;
import us.kbase.workspace.SaveObjectParams;
import us.kbase.workspace.WorkspaceClient;
import us.kbase.workspace.WorkspaceIdentity;

public class Workspace {

	private WorkspaceClient wsc ;
	public Workspace(String url, String auth) throws Exception{
		try{
		wsc = new WorkspaceClient(new URL(url),new AuthToken(auth));
		wsc.setAuthAllowedForHttp(true);
		}catch(Exception e) {
			e.printStackTrace();
		}
	}

	public WorkspaceClient getWrcClient(){
		return this.wsc;
	}
	/**
	 * <p>createWorkSpace</p>
	 * <pre>
	 * Creates a new workspace.
	 * </pre>
	 */
	public void createWorkspace(WorkspaceClient wsc, String ws_name, String desc , String permission ){
		//WorkspaceClient wsc;
		try{
			//			wsc = new WorkspaceClient(new URL(authUrl),new AuthToken(authToken));
			//			wsc.setAuthAllowedForHttp(true);
			wsc.createWorkspace(new CreateWorkspaceParams().withWorkspace(ws_name).withGlobalread(permission).withDescription(desc));

		}catch(Exception e){
			e.toString();
		}
	}
	// change the return type.
	public void wrclistWorkspaceObjects(WorkspaceClient wsc, String ws_name,String type){
		List<Tuple12<String,String,String,Long,String,String,String,String,String,String,Map<String,String>,Long>> wslist;
		try {
			wslist = wsc.listWorkspaceObjects
					(new ListWorkspaceObjectsParams().withWorkspace(ws_name).withType(type));

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	/**
	 * <p>wrcsaveObject</p>
	 * <pre>
	 * Saves as object to the new workspace.
	 * </pre>
	 */
	public String wrcsaveObject(WorkspaceClient wsc, String ws_name, String type, String id,String jsonStr){
		Tuple12<String, String, String, Long, String, String, String, String, String, String, Map<String, String>, Long> wscinfo;
		String ret = null;
		try {
			wsc.setAuthAllowedForHttp(true);
			System.out.println("wsname is " + ws_name +"  id is " + id +" type" + type );
			wscinfo = wsc.saveObject(new SaveObjectParams().withWorkspace(ws_name).withId(id).withType(type).withData(UObject.fromJsonString(jsonStr)));
			ret = wscinfo.getE1();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return ret;
	}
	/**
	 * <p>GetWorkSpaceObject</p>
	 * <pre>
	 * Gets an object from the workspace.
	 * </pre>
	 */
	public void GetWorkSpaceObject(WorkspaceClient wsc , FileSystem fs, String ws_name, String id){
		try {
			FSDataOutputStream fsout;
			GetObjectOutput out = wsc.getObject(new GetObjectParams().withWorkspace(ws_name).withId(id));
			String outStr  = out.getData().toJsonString();
			// include code to load this to hadoop fileSystem

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();

		}
	}
	/**
	 * <p>deleteWorkSpace</p>
	 * <pre>
	 * Deletes a workspace.
	 * </pre>
	 */
	public void deleteWorkSpace(WorkspaceClient wsc , String ws_name){
		try {
			wsc.deleteWorkspace(new WorkspaceIdentity().withWorkspace(ws_name));
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
