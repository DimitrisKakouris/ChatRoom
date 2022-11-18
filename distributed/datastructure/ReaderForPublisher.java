package distributed.datastructure;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Scanner;

public class ReaderForPublisher {
    public static final String TOPICS_FILE_PATH, BROKERS_FILE_PATH ;

    static {
        TOPICS_FILE_PATH = "resources/pub.txt";
        BROKERS_FILE_PATH = "resources/brok.txt";
    }

    private final ArrayList<Topic> topicsInterested;
    private final ArrayList<BrokerObj> brokerObjs;
    private final ArrayList<Data> messages_history;
    private Data lastMessage;


    //This contructor will use default files if their not passed at construction
    public ReaderForPublisher(String topicsfilepath, String brokersFilepath){
        if(topicsfilepath == null){
            topicsfilepath = TOPICS_FILE_PATH;
        }
        if(brokersFilepath == null){
            brokersFilepath = BROKERS_FILE_PATH;
        }
        String delimiter = ":";
        topicsInterested = parseLinesIntoTopicObjectsNotDelim(readAllLinesAndSplitNotDelim(topicsfilepath));
        brokerObjs = parseLinesIntoBrokerObjectsDelim(readAllLinesAndSplitDelim(brokersFilepath,delimiter));
        messages_history = getDataAlternateFromATopicsList(topicsInterested);

        lastMessage = null;
    }

    public void messageReceived(Data msg) {
        lastMessage = msg;
        messages_history.add(msg);
    }

    public Data getLastMessageIfReceived() {
        return lastMessage;
    }

    public void markMessageSent() {
        lastMessage = null;
    }

    public ArrayList<Topic> getTopicsInterested() {
        return topicsInterested;
    }

    public ArrayList<BrokerObj> getBrokerObjs() {
        return brokerObjs;
    }

    public ArrayList<Data> getmessages_history() {
        return messages_history;
    }

    public static ArrayList<String> readAllLinesAndSplitNotDelim(String filename) {

        // Declare and initialize variable that will store the result information
        // Each readLine element contains a String array that contains each comma-delimited sub-string
        ArrayList<String> readLines = new ArrayList<>();

        // Declare and open input file
        FileReader file = null;
        Scanner input = null;
        String line;
        try {
            file = new FileReader(filename);
            input = new Scanner(new BufferedReader(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        }

        // Read all data from the file and store them into the readLines
        try {
            while (input.hasNextLine()) {
                line = input.nextLine().trim();
                    readLines.add(line);
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        finally {
            // Close file
            try {
                input.close();
                file.close();
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }

        return readLines;
    }

    public static ArrayList<String[]> readAllLinesAndSplitDelim(String filename, String delimiter) {

        // Declare and initialize variable that will store the result information
        // Each readLine element contains a String array that contains each comma-delimited sub-string
        ArrayList<String[]> readLines = new ArrayList<>();

        // Declare and open input file
        FileReader file = null;
        Scanner input = null;
        String line;
        try {
            file = new FileReader(filename);
            input = new Scanner(new BufferedReader(file));
        } catch (FileNotFoundException e) {
            e.printStackTrace();
            return null;
        }

        // Read all data from the file and store them into the readLines
        try {
            while (input.hasNextLine()) {
                line = input.nextLine().trim();
                if (!line.equals("")) {
                    readLines.add(line.split(delimiter));
                }
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        finally {
            // Close file
            try {
                input.close();
                file.close();
            } catch (IOException e) {
                e.printStackTrace();
                return null;
            }
        }

        return readLines;
    }

    public static ArrayList<Topic> parseLinesIntoTopicObjectsNotDelim(ArrayList<String> lines){
        ArrayList<Topic> topics = new ArrayList<>();
        for(String line : lines){
            Topic topic = new Topic(line.trim());
            topics.add(topic);
        }
        return topics;
    }

    public static ArrayList<BrokerObj> parseLinesIntoBrokerObjectsDelim(ArrayList<String[]> lines){
        ArrayList<BrokerObj> brokerObjs = new ArrayList<>();
        for(String[] line : lines){
            BrokerObj brokerObj = new BrokerObj(line[0].trim(),Integer.parseInt(line[1].trim()));
            brokerObjs.add(brokerObj);
        }
        return brokerObjs;
    }


    public static ArrayList<Data> getDataFromATopicsList(ArrayList<Topic> topics){
        ArrayList<Data> messages = new ArrayList<>();
        return messages;
    }
    // Currently works for two topics
    //Epistefei Data objs enallax gia ta topics poy endiaferomaste.
    public static ArrayList<Data> getDataAlternateFromATopicsList(ArrayList<Topic> topics){

        ArrayList<Data> messages = new ArrayList<>();
        messages.add(new Data("Bot 1", new Topic("groupname1"), new Value("Hello from bot 1!"), ""));
        return messages;
    }



}
