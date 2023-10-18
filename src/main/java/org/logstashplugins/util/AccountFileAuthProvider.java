package org.logstashplugins.util;

import tech.ydb.auth.AuthIdentity;
import tech.ydb.auth.AuthProvider;

public class AccountFileAuthProvider implements AuthProvider  {
    private final AccountFileIdentity identity;

    public AccountFileAuthProvider(String pathToFile) {
        this.identity = new AccountFileIdentity(pathToFile);
    }

    @Override
    public AuthIdentity createAuthIdentity() {
        return identity;
    }

    private static final class AccountFileIdentity implements AuthIdentity {
        private final String pathToFile;

        private AccountFileIdentity(String pathToFile) {
            this.pathToFile = pathToFile;
        }

        @Override
        public String getToken() {
            return pathToFile;
        }
    }
}
