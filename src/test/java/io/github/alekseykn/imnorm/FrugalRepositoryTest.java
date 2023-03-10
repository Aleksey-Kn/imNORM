package io.github.alekseykn.imnorm;

import org.junit.jupiter.api.BeforeAll;
import support.dto.Dto;
import support.dto.DtoWithGenerateId;

public class FrugalRepositoryTest extends RepositoryTest {
    @BeforeAll
    static void setRepository() {
        repository = DataStorage.getDataStorage()
                .getStrictlyFrugalRepositoryForClass(Dto.class, 10);
        withGenerateIdRepository = DataStorage.getDataStorage()
                .getStrictlyFrugalRepositoryForClass(DtoWithGenerateId.class, 10);
    }
}
