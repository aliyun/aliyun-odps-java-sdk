package com.aliyun.odps.account;

import static org.junit.Assert.*;

import com.aliyun.odps.account.Account.AccountProvider;

import org.junit.Test;

public class AppAccountTest {
    @Test
    public void testGetType() {
        AppAccount account = new AppAccount(new ApsaraAccount("access_id", "access_key"));
        assertEquals(AccountProvider.APSARA, account.getType());
    }
}