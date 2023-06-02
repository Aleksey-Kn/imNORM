package support.dto;

import io.github.alekseykn.imnorm.annotations.GeneratedValue;
import io.github.alekseykn.imnorm.annotations.Id;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@Getter
@EqualsAndHashCode
@AllArgsConstructor
@ToString
public class DtoWithGenerateId {
    @Id
    @GeneratedValue(startId = 300)
    private int id;

    private final int number;

    public DtoWithGenerateId(final int n) {
        number = n;
    }
}
