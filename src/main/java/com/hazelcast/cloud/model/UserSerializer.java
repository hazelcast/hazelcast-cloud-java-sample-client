package com.hazelcast.cloud.model;

import com.hazelcast.cloud.model.User;
import com.hazelcast.nio.serialization.compact.CompactReader;
import com.hazelcast.nio.serialization.compact.CompactSerializer;
import com.hazelcast.nio.serialization.compact.CompactWriter;

public class UserSerializer implements CompactSerializer<User> {
    @Override
    public User read(CompactReader compactReader) {
        User u = new User();
        u.setCountry(compactReader.readString("country"));
        u.setName(compactReader.readString("name"));
        return u;
    }

    @Override
    public void write(CompactWriter compactWriter, User user) {
        compactWriter.writeString("name", user.getName());
        compactWriter.writeString("country", user.getCountry());
    }

    @Override
    public String getTypeName() {
        return "user";
    }

    @Override
    public Class<User> getCompactClass() {
        return User.class;
    }
}
