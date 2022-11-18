package distributed.datastructure;

import java.util.ArrayList;

public class TextIntoDataObject {

    public static ArrayList<Data> listWithDataObjsFromTxt(){

        ArrayList<Data> dataArrayList = new ArrayList<>();






        return dataArrayList;
    }

    public static ArrayList<Data> listWithDataForSpecificTopics(ArrayList<Topic> topics){

        ArrayList<Data> data = new ArrayList<>();

        for(Data dataall : listWithDataObjsFromTxt()){
            for(Topic topic : topics){
                if(topic.getGroupName().equals(dataall.getKey().getGroupName())){
                    data.add(dataall);
                }
            }
        }

        return data;
    }


}
