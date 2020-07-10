package com.dfec.kudu.kudusink2;

/**
 * \* Created with IntelliJ IDEA.
 * \* User: yfl
 * \* Date: 2020/7/9
 * \* To change this template use File | Settings | File Templates.
 * \* Description:
 * \
 */
public class TestPojo {
    private Integer id;
    private String name;

    public TestPojo() {
    }

    public TestPojo(Integer id, String name) {
        this.id = id;
        this.name = name;
    }

    public Integer getId() {
        return id;
    }

    public void setId(Integer id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    @Override
    public String toString() {
        return "TestPojo{" +
                "id=" + id +
                ", name='" + name + '\'' +
                '}';
    }
}