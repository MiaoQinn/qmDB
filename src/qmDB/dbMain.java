package qmDB;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class dbMain {
	static Statement statement = new Statement(null,null); 
	static ArrayList<byte[]> pages = new ArrayList<byte[]>();
	// enum class for output 
	public static enum MetaCommandResult {
		 META_COMMAND_SUCCESS,
		  META_COMMAND_UNRECOGNIZED_COMMAND 
	}
	public static enum PrepareResult {
		 PREPARE_SUCCESS,  
		 PREPARE_SYNTAX_ERROR,
		 PREPARE_UNRECOGNIZED_STATEMENT
	}
	public static enum StatementType {
		 STATEMENT_INSERT,
		 STATEMENT_SELECT 
	}  
	public static enum ExecuteResult{
		 EXECUTE_TABLE_FULL,
		 EXECUTE_SUCCESS
	}
	// row data structure, for now let us just hardcode everything  
		public static class Row{
			int id; 
			String username; 
			String email; 
			public Row(int id,String username, String email)
			{
				this.id=id; 
				this.username=username; 
				this.email=email;
			}
		}
	// statement class 
	public static class Statement{
		String sql; 
		StatementType statementtype; 
		Row row_to_insert;
		public Statement(String sql, StatementType statementtype)
		{
			this.sql=sql; 
			this.statementtype=statementtype;
		}
	}
	
	/*
	 *  insert statement 
	 */ 
	public static ExecuteResult execute_insert(Statement statement, Table table) throws IOException
	{
		if(table.num_rows>=TABLE_MAX_ROWS)
		{
			return  ExecuteResult.EXECUTE_TABLE_FULL;
		}
		Row row_to_insert = statement.row_to_insert;
		serialize_row(row_to_insert,table); 
		// is it possible to ++ in function 
		table.num_rows++;
		return ExecuteResult.EXECUTE_SUCCESS;
	}
	
	/*
	 * select statement 
	 */
	public static ExecuteResult excute_select(Statement statement,Table table)
	{
		List<Row> res= deserialize_row(table);  
		for(int i=0;i<res.size();i++)
		{
			System.out.println(res.get(i).id + "     "+res.get(i).username+"     "+res.get(i).email);
		}
		return ExecuteResult.EXECUTE_SUCCESS;
	}
	// Execute the sql statement 
	public static void execute_statement(Statement statement, Table table) throws IOException
	{
		switch(statement.statementtype)
		{
		case  STATEMENT_INSERT:
			System.out.println("This is where we will implement insert"); 
			execute_insert(statement,table);
			break;
		case  STATEMENT_SELECT:
			System.out.println("This is where we will implement select"); 
			excute_select(statement,table);
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
		String query=statement.sql;
		if(statement.sql.length()<6)
			 return PrepareResult.PREPARE_UNRECOGNIZED_STATEMENT;
		String tp = statement.sql.substring(0,6); 
		//System.out.println("preparestatement reseult: " +tp.compareTo("select"));
		// insert  
		if(tp.compareTo("insert")==0 || tp.compareTo("INSERT")==0)
		{
			statement.statementtype=StatementType.STATEMENT_INSERT; 
			// parse the element 
			String[] splitres=query.split("\\s+");  
			if(splitres.length!=4)
			{
				return PrepareResult.PREPARE_SYNTAX_ERROR;
			}
			Row row_to_insert = new Row(Integer.parseInt(splitres[1]), splitres[2],splitres[3]); 
			statement.row_to_insert=row_to_insert;
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
	 * Compact representation of a row
	 * 
	 */
	final static Integer ID_SIZE = 4; 
	final static Integer USERNAME_SIZE = 32; 
	final static Integer EMAIL_SIZE = 255; 
	final static Integer ID_OFFSET= 0; 
	final static Integer USERNAME_OFFSET= ID_OFFSET+ID_SIZE;
	final static Integer EMAIL_OFFSET= USERNAME_OFFSET+USERNAME_SIZE;
	final static Integer ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;
	/*
	 * 
	 * Table creation 
	 * 
	 */ 
	 final static Integer PAGE_SIZE=4096;
	 final static Integer TABLE_MAX_PAGES=100;
	 final static Integer ROW_PER_PAGE=PAGE_SIZE/291;
	 final static Integer TABLE_MAX_ROWS=ROW_PER_PAGE*TABLE_MAX_PAGES; 
	 
	 public static class Table{
		 public int num_rows=0; 
		 public  Table(int num_rows)
		 {
			 this.num_rows=num_rows;
		 }
	 }
	 
	 /*
	  * 
	  * Determine the start of free space
	  * 
	  */
	 public static int[] row_slot(Table table,int  row_num)
	 {
		 // locate page 
		 int page_num = row_num/ROW_PER_PAGE; 
		 // allocate one more page if the specified row_num crosses pages. This would only happen when inserting 
		 if(page_num>=pages.size())
		 {
			 pages.add(new byte[PAGE_SIZE]);
		 }
		 int row_offset = row_num % ROW_PER_PAGE; 
		 int byte_offset = row_offset*ROW_SIZE;
		 // res[0] is the page_num and res[1] is the byte_offset 
		 return new int[] {page_num,byte_offset};
	 } 
	 
	 /*
	  * 
	  * 
	  * write to page(byte array) for INSERT operation
	  * 
	  * 
	  */
	 public static void serialize_row(Row row_to_insert, Table table) throws IOException
	 {
		 int[] tp = row_slot(table,table.num_rows);
		 int page_num=tp[0]; 
		 int byte_offset=tp[1];  
		 // byte array to write  
		 
		 // byte array for integer
		 ByteBuffer idbb=ByteBuffer.allocate(4);
		 idbb.putInt(row_to_insert.id);   
		 // padding for username and 
		 //String.format("%-" + n + "s", s);  
		 // pad the username 
		 String username_pad=String.format("%-" + 32 + "s", row_to_insert.username); 
		 String email_pad=String.format("%-" + 255 + "s", row_to_insert.email);  
		 
		 // byte array for paremeter
		 byte[] a=idbb.array();  
		 byte[] b=username_pad.getBytes(); 
		 byte[] c=email_pad.getBytes();
		 ByteArrayOutputStream outputStream = new ByteArrayOutputStream( );
		 outputStream.write(a);
		 outputStream.write(b);
		 outputStream.write(c);
		 byte[] all=outputStream.toByteArray(); 
		 // assgin each bytet of the all in 
		 for(int i=byte_offset;i<byte_offset+ROW_SIZE;i++)
		 {
			 pages.get(page_num)[i]=all[i-byte_offset];
		 }
	 }
	
	 /*
	  * 
	  * 
	  * deserialize the byte array, for now just select all 
	  * 
	  */
	 public static List<Row> deserialize_row(Table table) { 
		 
		 ArrayList<Row> res = new ArrayList<Row>();
		 for(int i=0;i<pages.size();i++)
		 {
			 // read rows from each page, for each row
			 for(int j=0;j<PAGE_SIZE && j/ROW_SIZE<ROW_PER_PAGE && j/ROW_SIZE<table.num_rows;j+=ROW_SIZE) { 
				 // first 4 byte is the integer 
				 byte[] id_arr=new byte[4]; 
				 byte[] username_arr=new byte[32]; 
				 byte[] email_arr=new byte[255]; 
				 // for id 
				 for(int p=j+ID_OFFSET;p<j+USERNAME_OFFSET;p++)
				 {
					 id_arr[p-j]=pages.get(i)[p];
				 }
				 int id = ByteBuffer.wrap(id_arr).getInt(); 
				 // for username 
				 for(int m=j+USERNAME_OFFSET;m<j+EMAIL_OFFSET;m++)
				 {
					 username_arr[m-j-USERNAME_OFFSET] = pages.get(i)[m];
				 }
				 String username= new String(username_arr);
				 username=username.trim();
				 // for email  
				 for(int q=j+EMAIL_OFFSET;q<j+ROW_SIZE;q++)
				 {
					 email_arr[q-j-EMAIL_OFFSET] = pages.get(i)[q];
				 }
				 String email= new String(email_arr);
				 email=email.trim(); 
				 Row currow = new Row(id,username,email); 
				 res.add(currow);
			 }
		 }
		 return res;
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
	 
	 Table table = new Table(0);
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
  				execute_statement(statement,table);
  				System.out.println("right now there are no.: "+pages.size()+" of pages.");
  				System.out.println("Executed");
  				break;
  			case PREPARE_UNRECOGNIZED_STATEMENT:
  				//System.out.println(statement.statementtype==null);
  				System.out.println("Unrecognized sql command error.");
  				break;
  			case PREPARE_SYNTAX_ERROR: 
  				System.out.println("Syntax error. Could not parse statement.");
  				break;
  		}
  	}
 }
}