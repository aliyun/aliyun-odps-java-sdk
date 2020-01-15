package com.aliyun.odps;

import com.aliyun.odps.commons.transport.Response;
import com.aliyun.odps.rest.ResourceBuilder;
import com.aliyun.odps.rest.RestClient;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * Created by dongxiao on 2019/8/26.
 */
public class Sessions implements Iterable<Session.SessionItem> {

    private static Gson gson = new GsonBuilder().disableHtmlEscaping().create();
    private RestClient client;
    private Odps odps;

    public Sessions(Odps odps) {
        this.odps = odps;
        this.client = odps.getRestClient();
    }

    private class SessionListIterator extends ListIterator<Session.SessionItem> {
        private boolean inited = false;
        @Override
        protected List<Session.SessionItem> list() {
            // TODO add limit for list api, there is no need to do it currently
            if (inited) {
                return null;
            }
            List<Session.SessionItem> list = new ArrayList<>();
            try {
                Response resp = client.request(ResourceBuilder.buildSessionsResource(odps.getDefaultProject()), "GET", null, null, null);
                Type type = new TypeToken<Map<String, Session.SessionItem>>(){}.getType();
                String body = new String(resp.getBody());
                Map<String, Session.SessionItem> sessions = gson.fromJson(body, type);
                for (Map.Entry<String, Session.SessionItem> entry : sessions.entrySet()) {
                    list.add(entry.getValue());
                }
                inited = true;
            } catch (OdpsException e) {
                throw new RuntimeException(e.getMessage(), e);
            }
            return list;
        }
    }
    /**
     * 获取默认{@link Project}的所有表信息迭代器
     *
     * @return {@link SessionItem}迭代器
     */
    @Override
    public Iterator<Session.SessionItem> iterator() {
        return new SessionListIterator();
    }

    /**
     * 获得表信息迭代器 iterable
     *
     * @return {@link SessionItem}迭代器
     */
    public Iterable<Session.SessionItem> iterable() {
        return new Iterable<Session.SessionItem>() {
            @Override
            public Iterator<Session.SessionItem> iterator() {
                return new SessionListIterator();
            }
        };
    }
}
