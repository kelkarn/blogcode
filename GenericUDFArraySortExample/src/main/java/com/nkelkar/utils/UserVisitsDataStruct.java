package com.nkelkar.utils;

/**
 * Created with IntelliJ IDEA.
 * User: nkelkar
 * Date: 9/25/13
 * Time: 5:03 PM
 * To change this template use File | Settings | File Templates.
 */
public class UserVisitsDataStruct implements Comparable<UserVisitsDataStruct> {

    private String user_id;
    private int num_visits;

    public UserVisitsDataStruct(String user_id, int num_visits) {
        this.user_id = user_id;
        this.num_visits = num_visits;
    }

    public UserVisitsDataStruct() {}

    public String getUserId() {
        return this.user_id;
    }

    public int getNumVisits() {
        return this.num_visits;
    }

    @Override
    public int compareTo(UserVisitsDataStruct o) {
        return (this.num_visits == o.num_visits)?0:((this.num_visits < o.num_visits)?-1:1);
    }
}
