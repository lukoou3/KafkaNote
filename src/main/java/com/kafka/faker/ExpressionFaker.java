package com.kafka.faker;

// https://www.datafaker.net/documentation/expressions/#templatify
public class ExpressionFaker extends Faker<String>{
    private final String expression;
    private net.datafaker.Faker faker;

    public ExpressionFaker(String expression) {
        this.expression = expression;
    }

    @Override
    public void init(int instanceCount, int instanceIndex) throws Exception {
        faker = new net.datafaker.Faker();
    }

    @Override
    public String geneValue() throws Exception {
        return faker.expression(expression);
    }

    public static void main(String[] args) {
        net.datafaker.Faker faker = new net.datafaker.Faker();
        System.out.println(faker.expression("#{number.number_between '1','10'}"));
        System.out.println(faker.expression("#{Name.first_name}"));
        System.out.println(faker.expression("#{Name.name}"));
        System.out.println(faker.expression("#{internet.emailAddress}"));
    }
}
