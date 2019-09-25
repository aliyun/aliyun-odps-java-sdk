package com.aliyun.odps.account;

import static org.junit.Assert.*;

import com.aliyun.odps.account.Account.AccountProvider;
import org.apache.commons.codec.binary.Base64;
import org.junit.Test;

public class AppAccountTest {
    @Test
    public void testGetType() {
        AppAccount account = new AppAccount(new AliyunAccount("access_id", "access_key"));
        assertEquals(AccountProvider.ALIYUN, account.getType());
    }
}