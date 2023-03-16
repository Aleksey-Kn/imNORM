package io.github.alekseykn.imnorm;

import org.junit.jupiter.api.BeforeAll;
import support.StringDto;

public class FrugalRepositoryHashKeyTest extends HashKeyTest{
    @BeforeAll
    static void init() {
        repository = DataStorage.getDataStorage().getStrictlyFrugalRepositoryForClass(StringDto.class,
                100);
    }
}
