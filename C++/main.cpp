#include <iostream>
#include <fstream>
#include <sstream>
#include <vector>
#include <string>

#include "hs2client/api.h"

#include "kudu/client/callbacks.h"
#include "kudu/client/client.h"
#include "kudu/client/row_result.h"
#include "kudu/client/stubs.h"
#include "kudu/client/value.h"
#include "kudu/client/write_op.h"
#include "kudu/common/partial_row.h"
#include "kudu/util/monotime.h"

using kudu::KuduPartialRow;
using kudu::MonoDelta;
using kudu::Status;
using kudu::client::KuduClient;
using kudu::client::KuduClientBuilder;
using kudu::client::KuduColumnSchema;
using kudu::client::KuduError;
using kudu::client::KuduInsert;
using kudu::client::KuduPredicate;
using kudu::client::KuduScanBatch;
using kudu::client::KuduScanner;
using kudu::client::KuduSchema;
using kudu::client::KuduSchemaBuilder;
using kudu::client::KuduSession;
using kudu::client::KuduStatusFunctionCallback;
using kudu::client::KuduTable;
using kudu::client::KuduTableAlterer;
using kudu::client::KuduTableCreator;
using kudu::client::KuduValue;
using kudu::client::sp::shared_ptr;
using std::cin;
using std::cout;
using std::endl;
using std::string;
using std::vector;

int ingestData()
{

    const string master_addr = "cloudera:7051";
    const string table_name = "impala::default.test_table";
    int num_inserts = 0;

    try
    {
        shared_ptr<KuduClient> client;
        shared_ptr<KuduTable> table;
        shared_ptr<KuduSession> session;
        //shared_ptr<KuduInsert> insert;

        cout << "Creatting Client...\n";
        //Create and connecting to the client
        KuduClientBuilder()
            .add_master_server_addr(master_addr)
            .Build(&client);
        cout << "Client Created.\n";

        //Check if table is available
        cout << "Checking if table is available..\n";
        Status status = client->OpenTable(table_name, &table);
        if (!status.ok())
        {
            cout << "Failed to build client: " << status.message() << endl;
            return 1;
        }

        cout << "Found the table.\n";
        session = table->client()->NewSession();
        std::ifstream file;
        file.open("../research.csv");
        cout << "Reading file...\n";
        cout << "Inserting Values...\n";
        while (file.good())
        {
            string line;
            getline(file, line, '\n');
            vector<string> data;
            std::stringstream st(line);
            while (st.good())
            {
                string subs;
                getline(st, subs, ',');
                data.push_back(subs);
            }

            int id = std::stoi(data[0]);
            string tab = data[1];
            string brdwn = data[2];
            string sec_brdwn = data[3];
            int year = std::stoi(data[4]);
            int value = std::stoi(data[5]);
            string unit = data[6];

            //Inserting values line by line
            KuduInsert *insert = table->NewInsert();
            KuduPartialRow *row = insert->mutable_row();
            row->SetInt32("id", id);
            row->SetString("tab", tab);
            row->SetString("brdwn", brdwn);
            row->SetString("sec_brdwn", sec_brdwn);
            row->SetInt32("year", year);
            row->SetInt32("value", value);
            row->SetString("unit", unit);

            //executing the insert query
            session->Apply(insert);
            num_inserts += 1;
        }
        cout << "Insert Complete!!!\n";
        cout << "Total number of inserts : "<< num_inserts << endl;
        session->Close();
        return 0;
    }
    catch (const std::exception &e)
    {
        cout << "Error: " << e.what() << '\n';
    }
}

int readData()
{

    string host = "cloudera";
    string query = "SELECT * FROM test_table ORDER BY id";
    int port = 21050;
    int conn_timeout = 0;
    hs2client::ProtocolVersion protocol = hs2client::ProtocolVersion::HS2CLIENT_PROTOCOL_V7;
    std::unique_ptr<hs2client::Service> service;
    hs2client::Status status = hs2client::Service::Connect(host, port, conn_timeout, protocol, &service);
    if (!status.ok())
    {
        cout << "Failed to connect to service: " << status.GetMessage() << endl;
        service->Close();
        return 1;
    }

    string user = "";
    hs2client::HS2ClientConfig config;
    std::unique_ptr<hs2client::Session> session;
    status = service->OpenSession(user, config, &session);
    if (!status.ok())
    {
        cout << "Failed to execute select: " << status.GetMessage() << endl;
        session->Close();
        service->Close();
        return 1;
    }

    std::unique_ptr<hs2client::Operation> execute;
    status = session->ExecuteStatement(query, &execute);
    if (!status.ok())
    {
        cout << "Failed to execute: " << status.GetMessage() << endl;
        execute->Close();
        session->Close();
        service->Close();
        return 1;
    }

    std::unique_ptr<hs2client::ColumnarRowSet> results;
    bool has_more = true;
    int total_rows = 0;
    cout << "\n\t\tContents of test_table:\n";
    //int16_t i = 0;
    while (has_more)
    {
        status = execute->Fetch(&results, &has_more);
        if (!status.ok())
        {
            cout << "Failed to fetch results: " << status.GetMessage() << endl;
            execute->Close();
            session->Close();
            service->Close();
            return 1;
        }

        std::unique_ptr<hs2client::Int32Column> id_col = results->GetInt32Col(0);
        std::unique_ptr<hs2client::StringColumn> tab_col = results->GetStringCol(1);
        std::unique_ptr<hs2client::StringColumn> brdwn_col = results->GetStringCol(2);
        std::unique_ptr<hs2client::StringColumn> sec_brdwn_col = results->GetStringCol(3);
        std::unique_ptr<hs2client::Int32Column> year_col = results->GetInt32Col(4);
        std::unique_ptr<hs2client::Int32Column> value_col = results->GetInt32Col(5);
        std::unique_ptr<hs2client::StringColumn> unit_col = results->GetStringCol(6);

        assert(id_col->length() == tab_col->length());
        total_rows += id_col->length();
        for (int64_t i = 0; i < id_col->length(); i++)
        {
            cout << id_col->GetData(i) << "\t";
            cout << tab_col->GetData(i) << "\t";
            cout << brdwn_col->GetData(i) << "\t";
            cout << sec_brdwn_col->GetData(i) << "\t";
            cout << year_col->GetData(i) << "\t";
            cout << value_col->GetData(i) << "\t";
            cout << unit_col->GetData(i) << "\n";
        }

        cout<< "\nTotal Number of rows read : "<< total_rows << endl;

    }
    cout << endl;
    execute->Close();
    return 0;
}

int main()
{
    int x;
    cout << "Pres no '1' to ingest data \n";
    cout << "Pres no '2' to read data \n";
    cout << "Your option: ";
    cin >> x;

    if (x == 1)
    {
        ingestData();
    }
    else if (x == 2)
    {
        readData();
    }
    else
    {
        return 1;
    }
    return 0;
}
