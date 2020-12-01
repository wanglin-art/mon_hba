package org.monba;

import org.json.JSONObject;
import org.monba.commons.SpringHelp;
import org.monba.service.*;
import org.springframework.context.ApplicationContext;

/**
 * Hello world!
 */
public class App {
    public static void main(String[] args) {
        ApplicationContext applicationContext = SpringHelp.getApplicationContext();
        String key = args[0];
        if ("campaign".equals(key)) {
            campaignMeta(applicationContext);
        }else if("adgroup".equals(key)){
            adgroupMeta(applicationContext);
        }else if("keyword".equals(key)){ //最后
            keywordMeta(applicationContext);
        }else if("negative".equals(key)){  //不用
            negativeMeta(applicationContext);
        }else if("report_campaign".equals(key)){// ok2
            report_campaignMeta(applicationContext);
        }else if("report_adgroup".equals(key)){//ok2
            report_adgroupMeta(applicationContext);
        }else if ("report_keyword".equals(key)){//最后 ok2
            report_keywordMeta(applicationContext);
        }else if("report_searchterm".equals(key)){//最后  ok
            report_searchtermMeta(applicationContext);
        }else if ("mmp_campaign".equals(key)){ //0k2
            mmp_campaignMeta(applicationContext);
        }else if("mmp_adgroup".equals(key)){//ok2
            mmp_adgroupMeta(applicationContext);
        }else if("mmp_keyword".equals(key)){
            mmp_keywordMeta(applicationContext);

        }
    }

    private static void campaignMeta(ApplicationContext ac) {
        CampaignService campaignService = (CampaignService) ac.getBean("campaignService");
        campaignService.getData();
    }

    private static void adgroupMeta(ApplicationContext ac) {
        AdgroupService adgroupService = (AdgroupService) ac.getBean("adgroupService");
        adgroupService.getData();
    }
    private static void keywordMeta(ApplicationContext ac){
        KeywordService keywordService = (KeywordService) ac.getBean("keywordService");
        keywordService.getData();
    }
    private static void negativeMeta(ApplicationContext ac){
        NegativeService negativeService = (NegativeService) ac.getBean("negativeService");
        negativeService.getData();
    }
    private static void report_campaignMeta(ApplicationContext ac){
        Report_campaignService report_campaignService = (Report_campaignService) ac.getBean("report_campaignService");
        report_campaignService.getData();
    }
    private static void report_adgroupMeta(ApplicationContext ac){
        Report_adgroupService report_adgroupService = (Report_adgroupService) ac.getBean("report_adgroupService");
        report_adgroupService.getData();
    }
    private static void report_keywordMeta(ApplicationContext ac){
        Report_keywordService report_keywordService = (Report_keywordService) ac.getBean("report_keywordService");
        report_keywordService.getData();
    }
    private static void report_searchtermMeta(ApplicationContext ac){
        Report_searchtermService report_searchtermService = (Report_searchtermService) ac.getBean("report_searchtermService");
        report_searchtermService.getData();
    }
    private static void mmp_campaignMeta(ApplicationContext ac){
        Mmp_CampaignService mmp_CampaignService = (Mmp_CampaignService) ac.getBean("mmp_CampaignService");
        mmp_CampaignService.getData();
    }
    private static void mmp_adgroupMeta(ApplicationContext ac){
        Mmp_adgroupService mmp_adgroupService = (Mmp_adgroupService) ac.getBean("mmp_adgroupService");
        mmp_adgroupService.getData();
    }
    private static void mmp_keywordMeta(ApplicationContext ac){
        Mmp_keywordService mmp_keywordService = (Mmp_keywordService) ac.getBean("mmp_keywordService");
        mmp_keywordService.getData();
    }
}
