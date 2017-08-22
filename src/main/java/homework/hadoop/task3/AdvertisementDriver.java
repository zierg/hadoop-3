package homework.hadoop.task3;

import eu.bitwalker.useragentutils.UserAgent;
import homework.hadoop.task3.mapping.ParamsExtractor;
import homework.hadoop.task3.mapping.UserAgentUtils;
import lombok.AccessLevel;
import lombok.experimental.FieldDefaults;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@FieldDefaults(level = AccessLevel.PRIVATE, makeFinal = true)
public class AdvertisementDriver {

    public static void main(String[] args) {
        UserAgent agent = UserAgent.parseUserAgentString("Mozilla/5.0 (Windows NT 5.1) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 SE 2.X MetaSr 1.0");
        log.info("OS: {}", agent.getOperatingSystem().getGroup().getName());

//        String TEST = "481a3572b254fac0a100b3264ae0dff0\t20131027105900443\t1\tCC3FELF7dBw\tMozilla/5.0 (Windows NT 5.1) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.84 Safari/535.11 SE 2.X MetaSr 1.0\t112.251.126.*\t146\t159\t2\t13625cb070ffb306b425cd803c4b7ab4\td2a55a0aaf1e75231290f9369b711aa1\tnull\t127598132\t728\t90\tOtherView\tNa\t133\t12627\t277\t133\tnull\t2261\t13800,10117,10102,10024,10006,10110,10133,10063";
        String TEST = "86666a3bff3d62f9980b0d6f41db6ff1\t20131027114101172\t1\tD95JrwD_wYk\tMozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.1; Trident/5.0; SLCC2; .NET CLR 2.0.50727; .NET CLR 3.5.30729; .NET CLR 3.0.30729; Media Center PC 6.0; Tablet PC 2.0)\t118.161.86.*\t393\t393\t1\tfcfacc09c5ef956632754ccff97c8d44\t6d854165e9871c236437db46e0c07cac\tnull\tmm_11402872_1272384_2783913\t250\t250\tFirstView\tNa\t0\t12620\t294\t82\tnull\t2261\tnull";
        ParamsExtractor.Params params = ParamsExtractor.getAdvertisementParams(TEST);
        log.info("Params: {}", params);
        log.info("OS: {}", UserAgentUtils.getOsType(params.getUserAgent()));
    }

    static Logger log = LoggerFactory.getLogger(AdvertisementDriver.class);
}
