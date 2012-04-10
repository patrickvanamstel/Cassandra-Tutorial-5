package nl.kaninefaten.cassandra.tutorial.t5;

import java.util.ArrayList;

/**
 * Example object for demonstration purposes.
 * 
 * @author Patrick van Amstel
 * @date 2012 04 10
 *
 */
public class ExampleModelObject {

	private String 	_key 		;
	private String 	_firstName 	;
	private String 	_middleName	;
	private String 	_lastName	;
	private String 	_emailAdress;
	private String 	_loginName	;
	private String 	_password	;
	private Integer _loginFailureCount		= new Integer(0);
	private Long 	_creationTime;
	private Long 	_updateTime	;
	
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("ExampleModelObject [ \n");
		builder.append("key : " + _key + "\n");
		builder.append("firstName :" + _firstName + "\n");
		builder.append("middleName :" + _middleName + "\n");
		builder.append("lastName :" + _lastName + "\n");
		builder.append("emailAdress :" + _emailAdress + "\n");
		builder.append("loginName	 :" + _loginName + "\n");
		builder.append("password	 :" + _password + "\n");
		builder.append("loginFailureCount	 :" + _loginFailureCount + "\n");
		builder.append("creationTime :" + _creationTime + "\n");
		builder.append("updateTime	 :" + _updateTime + "\n");
		builder.append("]\n");
		return builder.toString();
	}
	
	/**
	 * Escape fields before writing them to a file.
	 * 
	 * @param field - Field to escape
	 * @return escaped field.
	 */
	private String escape(String field){
		if (field == null){
			return "";
		}
		char [] chars = field.toCharArray();
		StringBuilder builder = new StringBuilder();
		
		for (char c : chars){
			if (c == '\\'){
				builder.append("\\\\");
			}else if (c == '\t'){
				builder.append("\\\t");
			}else{
				builder.append(c);
			}
		}
		return builder.toString();
	}
	
	/**
	 * Transforms object to tabString.
	 * @return escaped tabString of this object.
	 */
	public String toTabString() {
		StringBuilder builder = new StringBuilder();
		builder.append(escape(_key));
		builder.append('\t');
		builder.append(escape(_firstName));
		builder.append('\t');
		builder.append(escape(_middleName));
		builder.append('\t');
		builder.append(escape(_lastName));
		builder.append('\t');
		builder.append(escape(_emailAdress));
		builder.append('\t');
		builder.append(escape(_loginName));
		builder.append('\t');
		builder.append(escape(_password));
		builder.append('\t');
		builder.append(escape("" + _loginFailureCount));
		builder.append('\t');
		builder.append(escape("" + _creationTime));
		builder.append('\t');
		builder.append(escape("" + _updateTime));
		return builder.toString();
	}
	
	/**
	 * Factory for creating object from tabString.
	 * <p>
	 * Note: This is a sample class. So no checking for corrupt data.
	 * If you want to break it you can break it.
	 * 
	 * @param tabString - Tabstring of this object
	 * @return this object.
	 */
	public static ExampleModelObject fromTabString(String tabString){
		if (tabString == null || tabString.equals("")){
			return null;
		}
		ExampleModelObject exampleModelObject = new ExampleModelObject();
		char [] chars = tabString.toCharArray();
		ArrayList <StringBuilder> fields = new ArrayList<StringBuilder>();
		fields.add(new StringBuilder());
		for (int i = 0 ; i < chars.length ; i++){
			char c = chars[i];
			if (c == '\\'){
				// escape char
				if (i < (chars.length - 1)){
					char cPlus1 = chars[i+1];
					fields.get(fields.size() - 1).append(cPlus1);
				}else{
					// break
				}
				i++;
				continue;
			}
			if (c == '\t'){
				fields.add(new StringBuilder());
				continue;
			}
			fields.get(fields.size() - 1).append(c);
		}
		exampleModelObject.setKey(fields.get(0).toString());
		exampleModelObject.setFirstName(fields.get(1).toString());
		exampleModelObject.setMiddleName(fields.get(2).toString());
		exampleModelObject.setLastName(fields.get(3).toString());
		exampleModelObject.setEmailAdress(fields.get(4).toString());
		exampleModelObject.setLoginName(fields.get(5).toString());
		exampleModelObject.setPassword(fields.get(6).toString());
		exampleModelObject.setLoginFailureCount(new Integer(fields.get(7).toString()));
		exampleModelObject.setCreationTime(new Long(fields.get(8).toString()));
		exampleModelObject.setUpdateTime(new Long(fields.get(9).toString()));
		return exampleModelObject;
	}
	
	
	
	// Getters and setters
	public String getKey() {
		return _key;
	}
	public void setKey(String key) {
		this._key = key;
	}
	public String getFirstName() {
		return _firstName;
	}
	public void setFirstName(String firstName) {
		this._firstName = firstName;
	}
	public String getMiddleName() {
		return _middleName;
	}
	public void setMiddleName(String middleName) {
		this._middleName = middleName;
	}
	public String getLastName() {
		return _lastName;
	}
	public void setLastName(String lastName) {
		this._lastName = lastName;
	}
	public String getEmailAdress() {
		return _emailAdress;
	}
	public void setEmailAdress(String emailAdress) {
		this._emailAdress = emailAdress;
	}
	public String getPassword() {
		return _password;
	}
	public void setPassword(String password) {
		this._password = password;
	}
	public Integer getLoginFailureCount() {
		return _loginFailureCount;
	}
	public void setLoginFailureCount(Integer loginFailureCount) {
		this._loginFailureCount = loginFailureCount;
	}
	public Long getCreationTime() {
		return _creationTime;
	}
	public void setCreationTime(Long creationTime) {
		this._creationTime = creationTime;
	}
	public Long getUpdateTime() {
		return _updateTime;
	}
	public void setUpdateTime(Long updateTime) {
		this._updateTime = updateTime;
	}
	public String getLoginName() {
		return _loginName;
	}
	public void setLoginName(String loginName) {
		this._loginName = loginName;
	}
	
	
	
	
	
}
