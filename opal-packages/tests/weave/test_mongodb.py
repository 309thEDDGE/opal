import pymongo
import pytest

class TestMongo():
    #setup db client
    @pytest.fixture(scope="class")
    def client_setup(self):
        client = pymongo.MongoClient("mongodb", username="root", password="example")
        yield client
        #remove test_database
        client.drop_database('test_database')
        
    #insert data into the db and store the id    
    @pytest.fixture
    def dataId(self, client_setup, data):
       
        db = client_setup.test_database
        return db.developers.insert_one(data).inserted_id
    
    #create the test_database database
    @pytest.fixture
    def db_setup(self,client_setup):
        db = client_setup.test_database
        return db
    
    #create the test data
    @pytest.fixture
    def data(self):
        data = {
             "developer":"FirstName.LastName",
             "email":"FirstName.LastName@fakeEmail",
             "jira_ticket":1017,
             "tags": ["dev", "ops", "analysis"],
             }
        return data
        
    #create the developers collection    
    @pytest.fixture
    def dev_setup(self,db_setup):
        dev = db_setup.developers
        return dev

    #test when getting the one piece of data in the developers collection under the test_database that it matches the id of the one that was inserted
    def test_findOne(self,dev_setup,client_setup,dataId):
        foundData = dev_setup.find_one()
        assert foundData['_id'] == dataId
    
    #verify the developer data
    def test_findDeveloper(self,dev_setup,client_setup,data):
        foundData = dev_setup.find_one()
        assert foundData['developer'] == data['developer']
        
    #verify the email data    
    def test_findEmail(self,dev_setup,client_setup,data):
        foundData = dev_setup.find_one()
        assert foundData['email'] == data['email']
        
    #verify the jira ticket data    
    def test_findJira_ticket(self,dev_setup,client_setup,data):
        foundData = dev_setup.find_one()
        assert foundData['jira_ticket'] == data['jira_ticket']
    
    #verify the tag data
    def test_findTags(self,dev_setup,client_setup,data):
        foundData = dev_setup.find_one()
        assert foundData['tags'] == data['tags']