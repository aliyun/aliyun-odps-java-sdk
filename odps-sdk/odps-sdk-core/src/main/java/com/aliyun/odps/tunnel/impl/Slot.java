package com.aliyun.odps.tunnel.impl;

import com.aliyun.odps.tunnel.TunnelException;

class Slot {
    private String slot;
    private String ip;
    private int port;

    public Slot(String slot, String server, boolean reload) throws TunnelException {
        if (slot.isEmpty() || server.isEmpty()) {
            throw new TunnelException("Slot or Routed server is empty");
        }
        this.slot = slot;
        // check empty server ip on init
        // fallback to old server ip on reload
        setServer(server, !reload);
    }

    public String getSlot() {
        return slot;
    }

    public String getIp() {
        return ip;
    }

    public int getPort() {
        return port;
    }

    public String getServer() {
        return ip + ":" + port;
    }

    public boolean equals(Slot slot) {
        return this.slot == slot.slot &&
                this.ip == slot.ip &&
                this.port == slot.port;
    }

    public void setServer(String server, boolean checkEmptyIp) throws TunnelException {
        String [] segs = server.split(":");
        if (segs.length != 2) {
            throw new TunnelException("Invalid slot format: " + server);
        }
        if (segs[0].isEmpty()) {
            if (checkEmptyIp) {
                throw new TunnelException("Empty server ip: " + server);
            }
        } else {
            this.ip = segs[0];
        }
        this.port = Integer.valueOf(segs[1]);
    }
}
