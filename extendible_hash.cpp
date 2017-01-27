#include <iostream>
#include <sstream>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include<fstream>
#include <cmath>
#include <ctime>
#include <climits>
#include <algorithm>
#include <vector>
#include <string>
#include <queue>
#include <stack>
#include <list>
#include <bitset>
#include <set>
#include <map>
#include <functional>
#include <numeric>
#include <utility>
#include <iomanip>
using namespace std;
typedef long long int ll;
typedef long double ld;
#define rep(i,a,b) for(ll i=a;i<=b;++i)
#define rev(i,a,b) for(ll i=a;i>=b;i--)
#define pll pair<ll,ll>
#define sll set<ll>
#define vll vector<ll>
#define vpll vector<pll>
#define F first
#define S second
#define pb push_back
#define mp make_pair
#define ln length()
#define M 1000000007
#define endl '\n'
#define FIO ios::sync_with_stdio(false);cin.tie(0);cout.tie(0)

int uniform_input_data_array[100000], HighBit_input_data_array[100000];
string Simulation_data_set="Uniform";


//Bucket class starts here
class Bucket
{
private:
  int number_of_record_it_can_hold = 10;
  int number_of_empty_space_record = 0;
  int local_index = 1;
  vector<int> records;//new vector<int>();
  bool is_end_of_file = false;
  bool is_over_flow_bucket = false;
  bool is_splitted_once = true;

public:
	Bucket()
	{

	}
 int index_of_overflow_bucket;


  Bucket(int nor,int local_index)
 {
	 	// cout<< is_splitted_once;
	 number_of_record_it_can_hold = nor;
	 number_of_empty_space_record = number_of_record_it_can_hold;
	 local_index = local_index;
	 records.resize(number_of_record_it_can_hold);
	 index_of_overflow_bucket = -1;
 }


  vector<int> getrecords()
 {
	 return records;
 }

  void set_index_of_overflow_bucket(int index)
 {
	 index_of_overflow_bucket = index;
 }

  bool get_is_splitted_once()
 {
	 return is_splitted_once;
 }
  void set_is_splitted_once(bool state)
 {
	 is_splitted_once = state;
 }


  int get_index_of_overflow_bucket()
 {
	 return index_of_overflow_bucket;
 }

  int getNumber_of_record_it_can_hold()
 {
	 return number_of_record_it_can_hold;
 }

  int getNumber_of_empty_space_record()
 {
	 return number_of_empty_space_record;
 }

  int getLocal_index()
 {
	 return local_index;
 }

  void setLocal_index(int index)
 {
	 local_index = index;
 }

  void addrecord(int key)
 {
	 records.push_back(key);
	 number_of_empty_space_record--;
 }

  void setRecords(vector<int> record)
 {
	 records = record;
 }



  bool isIs_end_of_file()
 {
	 return is_end_of_file;
 }

  void setIs_end_of_file(bool is_eof)
 {
	 is_end_of_file = is_eof;
 }

  bool isIs_over_flow_bucket()
 {
	 return is_over_flow_bucket;
 }

  void setIs_over_flow_bucket(bool is_bucket)
 {
	 is_over_flow_bucket = is_bucket;
 }

  void clear_all_old_records()
 {
	 records.clear();
	 number_of_empty_space_record = number_of_record_it_can_hold;
 }

};


//Extendible Hash class starts here    --- -----  >

class ExtentableHash
{
 int Global_index,local_index;
 int number_of_buckets = 1;//it is pow(2,Global_index)called B here
 int number_of_records_buckets_can_have;// called b here
 int RandomSearchQueryNumber[50];
 vector<int> Directory;
 vector<int> Directory_clone ;
 vector<Bucket> BucketCollection ;
 int bucket_index_start, directory_index_start, Bucketkey, Directorykey;
 Bucket tempbucket,tempbucket2;
 int count_split_dir, i,j, delimiter,delimiter_index, overflow_bucket_index_start, directory_index_in_vector;
 //Threshold of number of bucket in Main Memory
 const  int Main_Memory_Directory_Entries_Threshold =1024;
 const  int LEFT_MOST_BIT_START = 20;
 int bucketcount, number_of_records_in_hash_table, bucket_accessed_for_search, split_cost;

	public :
	ExtentableHash(int global_index, int bucket_capicity)
 {

	 Directory.resize(1024);
	 BucketCollection.resize(200000);
	 //Global_index = Global_index;
	 number_of_records_buckets_can_have = bucket_capicity;
	 number_of_buckets = (int)pow(2,Global_index);
	 bucket_index_start = directory_index_start = 0;
	 Directory.resize(1024);
	 delimiter = -1;
	 Global_index = local_index = 0;
	 BucketCollection.resize(200000);
	 overflow_bucket_index_start = 170000;
	 directory_index_in_vector = 195000;
	 number_of_records_in_hash_table = 0;
	 intializedDirectory();
	 bucketcount = number_of_buckets;
	 split_cost = 0;
 }




	void Dosimulation()
 {
	 if(Simulation_data_set =="Uniform")
		 for(int i=0;i<100000;i++)
			 DoInsertionSimulation(uniform_input_data_array[i],i%5000 == 0 && i>0);
	 else
		 for(int i=0;i<100000;i++)
		 {
			 cout<<(HighBit_input_data_array[i]+"     rno "+i);
			 DoInsertionSimulation(HighBit_input_data_array[i],i%5000 == 0 && i>0);
		 }

 }

	void DoInsertionSimulation(int key_record, bool is_search_query_to_be_executed)
 {
	 if(is_search_query_to_be_executed)
	 {
		 GenerateRandomQuery();
		 DosearchSimulation();
	 }
	 else
	 {
		 WriteSearchExtendibleHashCSV(number_of_records_in_hash_table,0);
	 }

	 number_of_records_in_hash_table++;

	 //Results: N/B*b vs N
	 WriteExtendibleHashCSV(number_of_records_in_hash_table, (float)number_of_records_in_hash_table/(bucketcount*number_of_records_buckets_can_have));
	 insert(key_record);

	 // Spliting Graph plot
	 WriteSplitCostExtendibleHashCSV(number_of_records_in_hash_table,split_cost);
	 split_cost = 0;

 }

	void insert(int key_record)
 {
	 Directorykey = getLeftMostBits(key_record, Global_index);
	 Bucketkey = Directory[Directorykey];
	 if(is_split(Bucketkey,key_record))
	 {
		 do_split(Directorykey,Bucketkey,key_record);
	 }
	 else
	 {
		 tempbucket = BucketCollection[Bucketkey];
		 while(tempbucket.get_index_of_overflow_bucket() != -1)
			 tempbucket = BucketCollection[tempbucket.get_index_of_overflow_bucket()];

		 tempbucket.addrecord(key_record);
	 }
 }

	void insert_during_rehash(int bucketkey,int key_record)
 {
	 Directorykey = getLeftMostBits(key_record, Global_index);
	 bucketkey = Directory[Directorykey];
	 Directorykey = getLeftMostBits(key_record, Global_index);
	 if(is_split(bucketkey,key_record))
	 {
		 insert_overflow_record(bucketkey, key_record);
	 }
	 else
	 {
		 tempbucket = BucketCollection[bucketkey];
		 while(tempbucket.get_index_of_overflow_bucket() != -1)
			 tempbucket = BucketCollection[tempbucket.get_index_of_overflow_bucket()];
		 tempbucket.addrecord(key_record);
	 }
 }



	void Rehash(int bucketkey)
 {

	 tempbucket = BucketCollection[bucketkey];
	 Directory_clone =  tempbucket.getrecords();
	 BucketCollection[bucketkey].clear_all_old_records();
	 if(tempbucket.get_index_of_overflow_bucket() != -1)
	 {
		 while(tempbucket.get_index_of_overflow_bucket() != -1)
		 {
			 tempbucket2 = BucketCollection[tempbucket.get_index_of_overflow_bucket()];
			 for(i=0 ; i<tempbucket2.getrecords().size();i++)
			 {
				 Directory_clone.push_back(tempbucket2.getrecords().at(i));
			 }
			 tempbucket2.clear_all_old_records();
			 tempbucket.set_index_of_overflow_bucket(-1);
			 tempbucket = tempbucket2;
		 }
	 }

	 for(i=0 ; i<Directory_clone.size();i++)
	 {
       insert_during_rehash(bucketkey,Directory_clone[i]);
	 }
	 Directory_clone.clear();
	 Directory_clone.resize(0);
 }

	void do_split(int directorykey,int bucketkey, int key_record)
 {
	 tempbucket = BucketCollection[bucketkey];
	 if(Global_index == tempbucket.getLocal_index() )
		 insert_directory_and_bucket(directorykey,bucketkey,key_record);
	 else if(Global_index > tempbucket.getLocal_index())
		 insert_only_bucket(directorykey,bucketkey,key_record);
	 else
		 insert_overflow_record(bucketkey,key_record);

	 Rehash(bucketkey);
	 Directorykey = getLeftMostBits(key_record, Global_index);
	 bucketkey = Directory[Directorykey];
	 insert_overflow_record(bucketkey,key_record);

 }

	void insert_overflow_record(int bucketkey,int key_record)
 {
	 tempbucket = BucketCollection[bucketkey];
	 while(tempbucket.get_index_of_overflow_bucket() != -1)
		 tempbucket = BucketCollection[tempbucket.get_index_of_overflow_bucket()];
	 if(tempbucket.getNumber_of_empty_space_record() > 0)
		 tempbucket.addrecord(key_record);
	 else
	 {
		 add_overflowbucket(bucketkey,key_record);
		 insert_overflow_record(bucketkey,key_record);
	 }
 }

	void insert_directory_and_bucket(int directorykey,int bucketkey,int key_record)
 {

	 Directory_clone.clear();
	 Directory_clone.resize(0);
	 for(i=0,j=0;i<directory_index_start;i++,j+=2)
	 {
		// Directory_clone.insertElementAt(Directory[i],j);
       Directory_clone.insert(Directory_clone.begin()+j, Directory[i]);
		 if(i == directorykey)
		 {
			// Directory_clone.insertElementAt(delimiter, j+1);
        Directory_clone.insert(Directory_clone.begin()+j+1, delimiter);
			 delimiter_index = j+1;
		 }
		 else
			 //Directory_clone.insertElementAt(Directory[i], j+1);
           Directory_clone.insert(Directory_clone.begin()+j+1, Directory[i]);
	 }
	 Global_index++;

	 if(directorykey >= Main_Memory_Directory_Entries_Threshold)
	 {
		 split_cost++;
	 }

	 Directory.clear();
	 Directory=Directory_clone;
	 directory_index_start*=2;


	//  Directory_clone.removeAllElements();
	//  Directory_clone.setSize(0);
Directory_clone.clear();
Directory_clone.resize(0);


	//  BucketCollection.removeElementAt(bucket_index_start);
	//  BucketCollection.insertElementAt(new Bucket(number_of_records_buckets_can_have,Global_index),bucket_index_start);

   BucketCollection.erase(BucketCollection.begin()+bucket_index_start);
   BucketCollection.insert(BucketCollection.begin()+bucket_index_start, *(new Bucket(number_of_records_buckets_can_have,Global_index)));

	 BucketCollection[bucketkey].setLocal_index(Global_index);
	 Directory.erase(Directory.begin()+delimiter_index);
	 Directory.insert(Directory.begin()+delimiter_index, bucket_index_start );
	 bucket_index_start++;
	 bucketcount++;
	 split_cost+=2;
}

 void insert_only_bucket(int directorykey,int bucketkey,int key_record)
 {
	 if(directorykey >= Main_Memory_Directory_Entries_Threshold)
		 split_cost++;

	 tempbucket = BucketCollection[bucketkey];
	 tempbucket.setLocal_index(tempbucket.getLocal_index()+1);
	 BucketCollection.erase(BucketCollection.begin()+bucket_index_start);
//	 BucketCollection.insert(BucketCollection.begin()+bucket_index_start, new Bucket(number_of_records_buckets_can_have,tempbucket.getLocal_index()));
BucketCollection.insert(BucketCollection.begin()+bucket_index_start, *(new Bucket(number_of_records_buckets_can_have,tempbucket.getLocal_index())));

	 bucketcount++;
	 count_split_dir = 1;
	 for(i=directorykey-1; i>=0; i--)
	 {
		 if(Directory[i] == bucketkey)
			 ++count_split_dir;
		 else
			 break;
	 }
	 for(j=directorykey+1;j<directory_index_start;j++)
	 {
		 if(Directory[j] == bucketkey)
			 ++count_split_dir;
		 else
			 break;
	 }

	 i++;
	 i+=count_split_dir/2;

	 for(;i<j;i++)
	 {

		 Directory.erase(Directory.begin()+i);
		 Directory.insert( Directory.begin()+i, bucket_index_start);
	 }
	 ++bucket_index_start;
	 split_cost+=2;

 }



	bool is_split(int bucketkey, int key_record)
 {
	 tempbucket = BucketCollection[bucketkey];
	 while(tempbucket.get_index_of_overflow_bucket() != -1)
		 tempbucket = BucketCollection[tempbucket.index_of_overflow_bucket];
	 if( tempbucket.getNumber_of_empty_space_record() ==  0)
		 return true;
	 return false;
 }



	void intializedDirectory()
 {
	 for(int i=0;i<number_of_buckets;i++)
	 {
		 BucketCollection.erase(BucketCollection.begin()+bucket_index_start);
		 BucketCollection.insert(BucketCollection.begin()+ bucket_index_start, *(new Bucket(number_of_records_buckets_can_have,local_index)));
		 Directory.erase(Directory.begin()+directory_index_start);
		 Directory.insert( Directory.begin()+directory_index_start, bucket_index_start);
		 ++bucket_index_start;
		 ++directory_index_start;
	 }
 }

	void DosearchSimulation()
 {
	 bucket_accessed_for_search = 0;
	 for(int i = 0;i<50;i++)
	 {
		 search(RandomSearchQueryNumber[i]);
	 }
	 WriteSearchExtendibleHashCSV(number_of_records_in_hash_table,(float)bucket_accessed_for_search/50);
 }

	void search(int key)
 {
	 Directorykey = getLeftMostBits(key,Global_index);
	 Bucketkey = Directory.at(Directorykey);
	 if(Directorykey >= 1024)
		 ++bucket_accessed_for_search;
	 tempbucket = BucketCollection.at(Bucketkey);
	// find(tempbucket.getrecords().begin(), tempbucket.getrecords().end(), key) != tempbucket.getrecords().end()
	 if(find(tempbucket.getrecords().begin(), tempbucket.getrecords().end(), key) != tempbucket.getrecords().end())
	 {
		 ++bucket_accessed_for_search;
		 return;
	 }
	 while(tempbucket.get_index_of_overflow_bucket() != -1)
	 {
		 ++bucket_accessed_for_search;
		 tempbucket = BucketCollection.at(tempbucket.get_index_of_overflow_bucket());
		 if(find(tempbucket.getrecords().begin(), tempbucket.getrecords().end(), key) != tempbucket.getrecords().end())
			 return;

	 }
 }


	int getLeftMostBits(int value, int n)
 {
	 if (n == 0) {
		 return 0;
	 }
	 else {
		 int rightShifts = LEFT_MOST_BIT_START - n;
	 	//  return value >>> rightShifts;
        return value >> rightShifts;
	 }
 }

	void GenerateRandomQuery() {


	 for(int i = 0;i<50;i++)
	 {
		 if(Simulation_data_set=="Uniform")
		 {
			 RandomSearchQueryNumber[i] = uniform_input_data_array[rand()%(number_of_records_in_hash_table)];
		 }
		 else
		 {
			 RandomSearchQueryNumber[i] = HighBit_input_data_array[rand()%(number_of_records_in_hash_table)];
		 }

	 }
 }


	void add_overflowbucket(int bucketkey,int key_record)
 {

	 int Hashkey;
	 tempbucket = *(new Bucket(number_of_records_buckets_can_have,0));
	 tempbucket.setIs_over_flow_bucket(true);
	 BucketCollection.erase(BucketCollection.begin()+overflow_bucket_index_start);
	 BucketCollection.insert(BucketCollection.begin()+overflow_bucket_index_start, tempbucket);
	 tempbucket = BucketCollection.at(bucketkey);
	 while(tempbucket.get_index_of_overflow_bucket() != -1 )
	 {
		 Hashkey = tempbucket.get_index_of_overflow_bucket();
		 tempbucket = BucketCollection.at(Hashkey);
	 }
	 tempbucket.set_index_of_overflow_bucket(overflow_bucket_index_start);
	 overflow_bucket_index_start++;
	 bucketcount++;
	 split_cost++;
 }

	void WriteExtendibleHashCSV(int x, float y)	{
string outputFile = "Result"+Simulation_data_set+"ExtendibleHash.csv";
bool alreadyExists = ifstream(outputFile);

	 ofstream myfile;
	      myfile.open (outputFile);
	      // myfile << "This is the first cell in the first column.\n";
	      // myfile << "a,b,c,\n";
	      // myfile << "c,s,v,\n";
	      // myfile << "1,2,3.456\n";
	      // myfile << "semi;colon";


	 try {
		 // use FileWriter constructor that specifies open for appending
		//  CsvWriter csvOutput = new CsvWriter(new FileWriter(outputFile, true), ',');

		 // if the file didn't already exist then we need to write out the header line
		 if (!alreadyExists)
		 {
			  myfile <<("X-labeled");
			  myfile<<("Y-Labeled");
			//  csvOutput.endRecord();
			  myfile<<"\n";
		 }
		 // else assume that the file already has the correct header line

		 // write out a few records
		 myfile<<""<<x;
		 myfile<<""<<y;
		 myfile<<"\n";

	 } catch (exception &e) {
		 e.what();
		     myfile.close();
	 }
	     myfile.close();
 }

	void WriteSearchExtendibleHashCSV(int x, float y)	{

	//  bool alreadyExists = new File(outputFile).exists();
	//  try {
	// 	 // use FileWriter constructor that specifies open for appending
	// 	 CsvWriter csvOutput = new CsvWriter(new FileWriter(outputFile, true), ',');
	 //
	// 	 // if the file didn't already exist then we need to write out the header line
	// 	 if (!alreadyExists)
	// 	 {
	// 		 csvOutput.write("X-labeled");
	// 		 csvOutput.write("Y-Labeled");
	// 		 csvOutput.endRecord();
	// 	 }
	// 	 // else assume that the file already has the correct header line
	 //
	// 	 // write out a few records
	// 	 csvOutput.write(""+x);
	// 	 csvOutput.write(""+y);
	// 	 csvOutput.endRecord();
	// 	 csvOutput.close();
	//  } catch (IOException e) {
	// 	 e.printStackTrace();
	//  }

		 string outputFile = "Result"+Simulation_data_set+"ExtendibleHash.csv";
        bool alreadyExists = ifstream(outputFile);
		 ofstream myfile;
		      myfile.open (outputFile);
		      // myfile << "This is the first cell in the first column.\n";
		      // myfile << "a,b,c,\n";
		      // myfile << "c,s,v,\n";
		      // myfile << "1,2,3.456\n";
		      // myfile << "semi;colon";


		 try {
			 // use FileWriter constructor that specifies open for appending
			//  CsvWriter csvOutput = new CsvWriter(new FileWriter(outputFile, true), ',');

			 // if the file didn't already exist then we need to write out the header line
			 if (!alreadyExists)
			 {
				  myfile <<("X-labeled");
				  myfile<<("Y-Labeled");
				//  csvOutput.endRecord();
				  myfile<<"\n";
			 }
			 // else assume that the file already has the correct header line

			 // write out a few records
			 myfile<<""<<x;
			 myfile<<""<<y;
			 myfile<<"\n";

		 } catch (exception &e ) {
			 e.what();
			     myfile.close();
		 }
		     myfile.close();

 }

	void WriteSplitCostExtendibleHashCSV(int x, int y) 	{
	 string outputFile = "ResultSplitCost"+Simulation_data_set+"ExtendibleHash.csv";
	 bool alreadyExists = ifstream(outputFile);

	 ofstream myfile;
 			 myfile.open (outputFile);
 		// 	 myfile << "This is the first cell in the first column.\n";
 		// 	 myfile << "a,b,c,\n";
 		// 	 myfile << "c,s,v,\n";
 		// 	 myfile << "1,2,3.456\n";
 		// 	 myfile << "semi;colon";


 	try {
 		// use FileWriter constructor that specifies open for appending
 	 //  CsvWriter csvOutput = new CsvWriter(new FileWriter(outputFile, true), ',');

 		// if the file didn't already exist then we need to write out the header line
 		if (!alreadyExists)
 		{
 			 myfile <<("X-labeled");
 			 myfile<<("Y-Labeled");
 		 //  csvOutput.endRecord();
 			 myfile<<"\n";
 		}
 		// else assume that the file already has the correct header line

 		// write out a few records
 		myfile<<""<<x;
 		myfile<<""<<y;
 		myfile<<"\n";

 	} catch (exception &e) {
 		e.what();
 				myfile.close();
 	}
 			myfile.close();


 }

	void print()
 {
	 int i,j;
	 for(i=0;i<bucket_index_start;i++)
	 {
		 Directory_clone = BucketCollection.at(i).getrecords();

		 for(j=0;j<Directory_clone.size();j++)
			 printf("%d ", Directory_clone.at(j));

		 if(BucketCollection.at(i).get_index_of_overflow_bucket() != -1)
		 {
			 tempbucket = BucketCollection.at(i);
			 while(tempbucket.get_index_of_overflow_bucket() != -1)
			 {
				 printf("-> ");
				 tempbucket = BucketCollection.at(tempbucket.get_index_of_overflow_bucket());
				 Directory_clone = tempbucket.getrecords();
				 for(j=0;j<Directory_clone.size();j++)
					 printf("%d ", Directory_clone.at(j));

			 }
		 }

		 cout<<endl;;

	 }
	 cout<<bucket_index_start <<" "<<Global_index<<" "<<overflow_bucket_index_start;
 }

};




	 	void generate_input_records() {
      for(int i = 0;i<100000;i++)
	 			uniform_input_data_array[i] = rand() % 800000;
     for(int i = 0;i<100000;i++)
	 		{
	 			if(i<60000)
	 				HighBit_input_data_array[i] = (rand() % 100000)+700000;
	 			else if(i<100000)
	 				HighBit_input_data_array[i] = rand() % 800000;
	     }
	 	}

	int main()
	{
    Bucket x(10,1);
		generate_input_records();
		 ExtentableHash eh(1,10);
		 eh.Dosimulation();
		 eh.print();
		 cout <<"Simulation Complete \n";
   return 0;
	}
