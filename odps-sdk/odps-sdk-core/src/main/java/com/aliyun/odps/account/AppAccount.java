package com.aliyun.odps.account;

/**
 * This class is for keeping track of the behaviors of a certain application.
 *
 * @author Jon (wangzhong.zw@alibaba-inc.com)
 */
public class AppAccount implements Account {
    private Account appAccount;
    private AppRequestSigner signer;

    public AppAccount(Account account) {
        switch (account.getType()) {
            case ALIYUN:
                this.appAccount = account;
                this.signer = new AppRequestSigner((AliyunAccount) account);
                break;
            default:
                throw new IllegalArgumentException("Unsupported account provider for application account.");
        }
    }

    @Override
    public AccountProvider getType() {
        return appAccount.getType();
    }

    @Override
    public AppRequestSigner getRequestSigner() {
        return signer;
    }
}
