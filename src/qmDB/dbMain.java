package qmDB;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashSet;

public class dbMain {
	static Statement statement = new Statement(null,null);
	// enum class for output 
	public static enum MetaCommandResult {
		 META_COMMAND_SUCCESS,
		  META_COMMAND_UNRECOGNIZED_COMMAND 
	}
	public static enum PrepareResult {
		 PREPARE_SUCCESS, 
		 PREPARE_UNRECOGNIZED_STATEMENT
	}
	public static enum StatementType {
		 STATEMENT_INSERT,
		 STATEMENT_SELECT 
	}  
	
	// statement class 
	public static class Statement{
		String sql; 
		StatementType statementtype; 
		public Statement(String sql, StatementType statementtype)
		{
			this.sql=sql; 
			this.statementtype=statementtype;
		}
	}
	// Execute the sql statement 
	public static void execute_statement(Statement statement)
	{
		switch(statement.statementtype)
		{
		case  STATEMENT_INSERT:
			System.out.println("This is where we will implement insert"); 
			break;
		case  STATEMENT_SELECT:
			System.out.println("This is where we will implement select"); 
			break;
		}
	} 
	
	// check if the input meta command is valid 
	public static MetaCommandResult do_meta_command(String statement)
	{
		HashSet<String> st = new HashSet<String>();  
		// .exit 
		st.add(".exit");
		if(st.contains(statement))
		{	
			if(statement.compareTo(".exit")==0)
			{	
				System.out.println("Exit the program");
				System.exit(0);
			}
			System.out.println("succefully recognized commmand: "+statement);
			return  MetaCommandResult.META_COMMAND_SUCCESS; 
		}
		return MetaCommandResult.META_COMMAND_UNRECOGNIZED_COMMAND;
	}
	
	// class for execute_statement 
	public static PrepareResult prepare_statement()
	{	
		if(statement.sql.length()<6)
			 return PrepareResult.PREPARE_UNRECOGNIZED_STATEMENT;
		String tp = statement.sql.substring(0,6); 
		//System.out.println("preparestatement reseult: " +tp.compareTo("select"));
		// insert  
		if(tp.compareTo("insert")==0 || tp.compareTo("INSERT")==0)
		{
			statement.statementtype=StatementType.STATEMENT_INSERT;
			return PrepareResult.PREPARE_SUCCESS;
		} 
		else if(tp.compareTo("select")==0 || tp.compareTo("SELECT")==0)
		{
			statement.statementtype=StatementType.STATEMENT_SELECT;
			return PrepareResult.PREPARE_SUCCESS;
		} 
		return PrepareResult.PREPARE_UNRECOGNIZED_STATEMENT;
	}
	
	
 /*
  * 
  * 
  * 
  * Main Function  
  * 
  * 
  * 
  */
 public static void main(String [] args) throws IOException
 {
	 while(true)
  	{ 
		 System.out.println("Please input the query you wish to conduct: ");
  		BufferedReader reader = new BufferedReader(new InputStreamReader(System.in)); 
  		String sql = reader.readLine();  
  		
  		/*
  		 *	Reactor to deal with various meta command  
  		 *
  		 */
  		
  		/*if(sql.compareTo(".exit")==0)
  		{ 
  			System.out.println("Exit the program");
  			break; 
  		}
  		else
  		{
  			System.out.println("Incorrect sql sentence of"+"'"+sql+"'");
  		}*/  
  		
  		/*
  		 * 
  		 * 
  		 * 
  		 *  Meta command block
  		 *  
  		 *  
  		 *  
  		 */
  		if(sql.charAt(0)=='.')
  		{
  			switch(do_meta_command(sql)) {
  			case META_COMMAND_SUCCESS:
  				System.out.println("meta command sucessfully recognized"); 
  				break;
  			case META_COMMAND_UNRECOGNIZED_COMMAND:
  				System.out.println("Unrecognized meta command error. "); 
  				break;
  			}
  			// next round 
  			continue;
  		}
  		
  		/*
  		 * 
  		 * 
  		 * 
  		 *  SQL command block
  		 *  
  		 *  
  		 *  
  		 */
  		//Statement statement = new Statement(sql,null); 
  		statement.sql=sql;
  		switch(prepare_statement())
  		{
  			case PREPARE_SUCCESS:
  				execute_statement(statement);
  				System.out.println("Executed");
  				break;
  			case PREPARE_UNRECOGNIZED_STATEMENT:
  				//System.out.println(statement.statementtype==null);
  				System.out.println("Unrecognized sql command error.");
  				break;
  		}
  	}
 }
}