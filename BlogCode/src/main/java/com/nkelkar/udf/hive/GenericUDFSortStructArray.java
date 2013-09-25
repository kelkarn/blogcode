package com.nkelkar.udf.hive;

import com.nkelkar.utils.UserVisitsDataStruct;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.*;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

import java.util.ArrayList;
import java.util.Collections;


/*
 * User: nkelkar
 * Date: 9/25/13
 * Time: 4:08 PM
 * Description: Takes in an array of structs, sorts it on a field and then returns a sorted array of structs
 */

public class GenericUDFSortStructArray extends GenericUDF {

    private ListObjectInspector loi;

    private StructObjectInspector structoi;

    private ArrayList<Object []> ret;

    private IntObjectInspector intoi = null;

    @Override
    public ObjectInspector initialize(ObjectInspector[] args) throws UDFArgumentException {

        switch (args.length) {  // decide how to respond on provision of variable arguments
            case 1:
                break;
            case 2:
                if (args[1].getCategory() != ObjectInspector.Category.PRIMITIVE) {
                    throw new UDFArgumentTypeException(0, "the function sortStructArray() takes in only a primitive type" +
                            " of data for the second argument, if provided");
                }

                if (((PrimitiveObjectInspector)args[1]).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT) {
                    throw new UDFArgumentTypeException(0, "the function sortStructArray() takes in only an int type" +
                            " of data for the second argument, if provided. " + args[1].getTypeName() + " found for the second argument instead");
                }
                intoi = (IntObjectInspector)args[1];
                break;
            default:
                throw new UDFArgumentLengthException("The function sortStructArray() takes in either one or two arguments");
        }


        if (args[0].getCategory() != ObjectInspector.Category.LIST) {
            throw new UDFArgumentTypeException(0, "the function sortStructArray() takes in only a list type" +
                    " of data for the first argument");
        }


        if (((ListObjectInspector)args[0]).getListElementObjectInspector().getCategory() != ObjectInspector.Category.STRUCT) {
            throw new UDFArgumentTypeException(0, "the function sortStructArray() takes in only a list<struct> type" +
                    " of data for the first argument. " + "However, list<" + ((ListObjectInspector)args[0]).getListElementObjectInspector().getCategory() + "> was found instead");
        }

        loi = (ListObjectInspector)args[0];

        structoi = (StructObjectInspector)loi.getListElementObjectInspector();

        // nkelkar --comment get handles for the fields within the struct
        StructField field_0 = structoi.getStructFieldRef("user_id");
        StructField field_1 = structoi.getStructFieldRef("num_visits");

        // nkelkar --comment are all the field names the same as we expect them to be?
        if (field_0 == null)
            throw new UDFArgumentTypeException(0,"No \"user_id\" field in input structure "+structoi.getTypeName());
        if (field_1 == null)
            throw new UDFArgumentTypeException(0,"No \"num_visits\" field in input structure "+structoi.getTypeName());

        ObjectInspector field0_oi = field_0.getFieldObjectInspector();
        ObjectInspector field1_oi = field_1.getFieldObjectInspector();

        if (field0_oi.getCategory() != ObjectInspector.Category.PRIMITIVE || field1_oi.getCategory() != ObjectInspector.Category.PRIMITIVE) {
            throw new UDFArgumentTypeException(0, "Struct elements passed in arg[0] were not found to be of STRUCT<PRIMITIVE, PRIMITIVE> type");
        }

        if (((PrimitiveObjectInspector)field0_oi).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.STRING ||
            ((PrimitiveObjectInspector)field0_oi).getPrimitiveCategory() != PrimitiveObjectInspector.PrimitiveCategory.INT) {
            throw new UDFArgumentTypeException(0, "Struct elements passed in arg[0] were not found to be of STRUCT<STRING, INT> type");
        }


        ret = new ArrayList<Object []>();

        ArrayList<String> structFieldNames = new ArrayList<String>();
        structFieldNames.add("user_id");
        structFieldNames.add("num_visits");

        ArrayList<ObjectInspector> structFieldObjectInspectors = new ArrayList<ObjectInspector>();
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableStringObjectInspector);
        structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);

        return ObjectInspectorFactory.getStandardListObjectInspector(ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames, structFieldObjectInspectors));
    }

    @Override
    public Object evaluate(DeferredObject[] arguments) throws HiveException {
        ret.clear();
        int top_k = -1;

        switch (arguments.length) {
            case 1:
                if (arguments[0] == null) return null;
                break;
            case 2:
                if (arguments[0] == null || arguments[1] == null) return null;
                top_k = intoi.get(arguments[1].get());
            default:
                return null;
        }

        if (top_k < 0)
            top_k = loi.getListLength(arguments[0].get());

        // nkelkar --comment first, read in the data
        ArrayList<UserVisitsDataStruct> intermediate_arr = new ArrayList<UserVisitsDataStruct>();

        for (int i=0; i<loi.getListLength(arguments[0].get()); i++) {
            Text temp_user_id = (Text)structoi.getStructFieldData(loi.getListElement(arguments[0].get(), i), structoi.getStructFieldRef("user_id"));
            IntWritable temp_num_visits = (IntWritable)structoi.getStructFieldData(loi.getListElement(arguments[0].get(), i), structoi.getStructFieldRef("num_visits"));

            intermediate_arr.add(new UserVisitsDataStruct(temp_user_id.toString(), temp_num_visits.get()));

        }

        Collections.sort(intermediate_arr);


        for (int i=0; i<top_k; i++) {
            Object [] ret_obj = new Object [2];
            ret_obj[0] = new Text(intermediate_arr.get(i).getUserId());
            ret_obj[1] = new IntWritable(intermediate_arr.get(i).getNumVisits());
            ret.add(ret_obj);
        }

        return ret;
    }

    @Override
    public String getDisplayString(String[] strings) {
        if (strings.length == 1)
            return "sortStructArray(" + strings[0] + ")";
        else
            return "sortStructArray(" + strings[0] + "," + strings[1] + ")";
    }
}
