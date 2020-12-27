package cn.pandadb.grpc.converter;
import cn.pandadb.grpc.*;

import java.util.List;
import java.util.Map;

public class TypeConverter {

    public static Node getNode(Long id, List<String> labels, Map<String, Object> properties){
        Node.Builder builder = Node.newBuilder();
        builder.setNodeId(id);
        labels.forEach(label -> builder.addNodeLabels(label));
        builder.setProperties(getProperty(properties));
        return builder.build();
    }
    public static Relationship getRelationship(Long id, Long startNodeId, Long endNodeId,
                                               List<String> labels, Map<String, Object> properties){
        Relationship.Builder builder = Relationship.newBuilder();
        builder.setRelationId(id).setStartNode(startNodeId).setEndNode(endNodeId);
        labels.forEach(label -> builder.addRelationLabels(label));
        builder.setProperties(getProperty(properties));
        return builder.build();
    }
    public static Property getProperty(Map<String, Object> properties){
        Property.Builder propertyBuilder = Property.newBuilder();
        properties.forEach(
                (k, v) -> {
                    if (v instanceof Integer){
                        propertyBuilder.addIntValue(getIntValue(k, (int) v));
                    }
                    else if (v instanceof Float){
                        propertyBuilder.addFloatValue(getFloatValue(k, (float) v));
                    }
                    else if (v instanceof String){
                        propertyBuilder.addStringValue(getStringValue(k, String.valueOf(v)));
                    }
                    else if (v instanceof Boolean){
                        propertyBuilder.addBoolValue(getBooleanValue(k, (boolean) v));
                    }
                    else {
                        try {
                            throw new UndefineValueConverterException("not implement this value converter");
                        } catch (UndefineValueConverterException e) {
                            e.printStackTrace();
                        }
                    }
                }
        );
        return propertyBuilder.build();
    }
    public static IntValue getIntValue(String key, int value){
        return IntValue.newBuilder().setKey(key).setValue(value).build();
    }
    public static FloatValue getFloatValue(String key, float value){
        return FloatValue.newBuilder().setKey(key).setValue(value).build();
    }
    public static StringValue getStringValue(String key, String value){
        return StringValue.newBuilder().setKey(key).setValue(value).build();
    }
    public static BooleanValue getBooleanValue(String key, boolean value){
        return BooleanValue.newBuilder().setKey(key).setValue(value).build();
    }
}

class UndefineValueConverterException extends Exception{
    public UndefineValueConverterException(){}
    public UndefineValueConverterException(String message){
        super(message);
    }
}