package support.dto;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import io.github.alekseykn.imnorm.annotations.Id;

@Getter
@AllArgsConstructor
@EqualsAndHashCode
public class StringDto {
    @Id
    private String id;
}
