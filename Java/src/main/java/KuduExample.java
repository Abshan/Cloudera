import org.apache.kudu.client.*;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.*;
import java.util.Scanner;

public class KuduExample{
    
    //Function to insert data into kudu tables
    static void IngestData(){
        KuduTable table;
        Insert insert;
        String row = "";
        System.out.println("Connecting to Database...");
        KuduClient kuduClient = new KuduClient.KuduClientBuilder("192.168.3.102:7051").build();
        System.out.println("Connection Succesful");
        KuduSession session = kuduClient.newSession();
        System.out.println("Session Created..");


        try {
            String tableName = "impala::default.test_table";
            //Opening the specific table
            System.out.println("Opening table..");
            table = kuduClient.openTable(tableName);
            System.out.println("Table opened...");

            //Opening the CSV File
            System.out.println("Reading File...");
            FileReader fr = new FileReader("../research1.csv");
            BufferedReader br = new BufferedReader(fr);
            System.out.println("Reading Succesfull");

            //Insert Files into kudu table
            int insert_no = 0;
            System.out.println("Inserting Files...");
            while ((row = br.readLine()) != null) {
                //Split & assign CSV data to variables
                String[] data = row.split(",");
                int id = Integer.parseInt(data[0]);
                String tab = data[1];
                String brdwn = data[2];
                String sec_brdwn = data[3];
                int year = Integer.parseInt(data[4]);
                int value = Integer.parseInt(data[5]);
                String unit = data[6];

                //Inserting values line by line
                insert = table.newInsert();
                insert.getRow().addInt("id", id);
                insert.getRow().addString("tab", tab);
                insert.getRow().addString("brdwn", brdwn);
                insert.getRow().addString("sec_brdwn", sec_brdwn);
                insert.getRow().addInt("year", year);
                insert.getRow().addInt("value", value);
                insert.getRow().addString("unit", unit);

                //Executing the insert query
                session.apply(insert);
                insert_no++;

            }
            //Closing the session
            session.close();
            System.out.println("File Ingestion Succesfull");
            System.out.println("No of inserted records: "+ insert_no);

        } catch (KuduException e) {
            e.printStackTrace();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    static void ReadData(){
        
        //Reading data from Kudu using Impala
        try {

            String jdbcUrl = "jdbc:impala://192.168.3.102:21050;AutoMech=0";
            String query = "select * from test_table;";
            //Driver Name
            Class.forName("com.cloudera.impala.jdbc.Driver");
            Connection con = DriverManager.getConnection(jdbcUrl);
            System.out.println("Connection Succesfull");
            Statement st = con.createStatement();
            System.out.println("Reading Data... \n\n");
            ResultSet rs = st.executeQuery(query);
            int no = 0;

            while (rs.next()){
                int id = rs.getInt("id");
                String tab = rs.getString("tab");
                String brdwn = rs.getString("brdwn");
                String sec_brdwn = rs.getString("sec_brdwn");
                int year = rs.getInt("year");
                int value = rs.getInt("value");
                String unit = rs.getString("unit");
                System.out.println(id+","+tab+","+brdwn+","+sec_brdwn+","+year+","+value+","+unit);
                no++;
            }
            System.out.print("\n\nNo of records: " + no);

            st.close();
            rs.close();
            con.close();

        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        } catch (SQLException throwables) {
            throwables.printStackTrace();
        }
    }

    public static void main(String[] args) {
        //Simple terminal ui to insert & read data from Kudu and Impala
        Scanner obj = new Scanner(System.in);
        System.out.println("..............................");
        System.out.println("Enter No. 1 to Insert Data.");
        System.out.println("Enter No. 2 to Read Data.");
        System.out.println("..............................");
        System.out.print("Enter the prefered option: ");
        int opt = obj.nextInt();
        if (opt == 1){
            IngestData();
        }else{
            ReadData();
        }
    }
}

