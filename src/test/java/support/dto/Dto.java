package support.dto;

import io.github.alekseykn.imnorm.annotations.Id;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;

@AllArgsConstructor
@Getter
@EqualsAndHashCode
@ToString
public class Dto {
    @Id
    private int id;
}
