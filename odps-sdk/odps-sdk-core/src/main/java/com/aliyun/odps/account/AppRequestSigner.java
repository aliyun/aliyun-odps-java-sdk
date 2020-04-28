package com.aliyun.odps.account;

import com.aliyun.odps.account.Account.AccountProvider;
import com.aliyun.odps.commons.transport.Headers;
import com.aliyun.odps.commons.transport.Request;
import com.aliyun.odps.utils.StringUtils;
import java.nio.charset.StandardCharsets;

import org.apache.commons.codec.binary.Base64;

/**
 * @author Jon (wangzhong.zw@alibaba-inc.com)
 */
public class AppRequestSigner implements RequestSigner {
    private Account account;

    public AppRequestSigner(Account account) {
        this.account = account;
    }

    @Override
    public void sign(String resource, Request req) {
        AccountProvider accountProvider = this.account.getType();
        switch(accountProvider) {
            case ALIYUN:
                // TODO: Case sensitive
                String providerStr = accountProvider.toString().toLowerCase();
                String signature = SecurityUtils.getApplicationSignature(
                    providerStr,
                    ((AliyunAccount) account).getAccessId(),
                    getAliyunSignature(req));
                req.getHeaders().put(Headers.APP_AUTHENTICATION, signature);
                break;
            default:
                throw new IllegalArgumentException("Unsupported account provider for application account.");
        }
    }

    public String getAliyunSignature(Request request) {
        String strToSign = request.getHeaders().get(Headers.AUTHORIZATION);
        return getAliyunSignature(strToSign);
    }

    public String getAliyunSignature(String strToSign) {
        if (StringUtils.isNullOrEmpty(strToSign)) {
            throw new RuntimeException("String to sign cannot be empty or null.");
        }

        byte[] crypto;
        crypto = SecurityUtils.hmacsha1Signature(
            strToSign.getBytes(StandardCharsets.UTF_8),
            ((AliyunAccount) account).getAccessKey().getBytes());

        return Base64.encodeBase64String(crypto).trim();
    }
}
