package support.dto;

import lombok.AllArgsConstructor;
import lombok.Getter;
import io.github.alekseykn.imnorm.annotations.Id;

@Getter
@AllArgsConstructor
public class StringDto {
    @Id
    private String id;
}
