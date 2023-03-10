package io.github.alekseykn.imnorm;

import org.junit.jupiter.api.BeforeAll;
import support.dto.Dto;

public class FrugalRepositoryTest extends RepositoryTest {
    @BeforeAll
    static void setRepository() {
        repository = DataStorage.getDataStorage()
                .getStrictlyFrugalRepositoryForClass(Dto.class, 100);
    }
}
