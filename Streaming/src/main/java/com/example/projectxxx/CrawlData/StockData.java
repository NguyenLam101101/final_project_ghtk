package com.example.projectxxx.CrawlData;

import org.openqa.selenium.By;
import org.openqa.selenium.Keys;
import org.openqa.selenium.WebDriver;
import org.openqa.selenium.WebElement;
import org.openqa.selenium.edge.EdgeDriver;
import org.openqa.selenium.support.ui.Select;

import java.util.Iterator;
import java.util.Random;
import java.util.Set;

public class StockData {
    private WebDriver webDriver;
    public String startDate = "01/01/2022";
    public String endDate = "01/05/2022";
    public String[] indexs = new String[] {"VN30-Index", "ACB", "BID", "BVH", "CTG", "FPT", "GAS", "GVR", "HDB", "HPG", "KDH", "MBB", "MSN",
    "MWG", "NVL", "PDR", "PLX", "POW", "SAB", "SSI", "STB", "TCB", "TPB", "VCB", "VHM", "VIB", "VIC", "VJC", "VNM", "VPB", "VRE"};

    public void crawl() throws InterruptedException {
        System.setProperty("webdriver.edge.driver", "src/main/java/com/example/projectxxx/EdgeDriver/msedgedriver.exe");
        webDriver = new EdgeDriver();
        webDriver.navigate().to("https://finance.vietstock.vn/trading-statistic");
        Random rd = new Random();


        WebElement login = webDriver.findElement(By.xpath("/html/body/div[2]/div[6]/div/div[2]/div[2]/a[3]"));
        login.click();

        Thread.sleep(rd.nextInt(1000) + 1000);

        WebElement email = webDriver.findElement(By.xpath("/html/body/div[2]/div[8]/div/div/div/form/div[1]/div[1]/div[1]/input"));
        email.sendKeys("nguyenvanlam10112001@gmail.com");

        WebElement password = webDriver.findElement(By.xpath("/html/body/div[2]/div[8]/div/div/div/form/div[1]/div[1]/div[3]/input"));
        password.sendKeys("NguyenLam2001");

        webDriver.findElement(By.xpath("/html/body/div[2]/div[8]/div/div/div/form/div[1]/div[2]/div[1]/button")).click();

        Thread.sleep(rd.nextInt(1000) + 2000);

        for (String stock:indexs) {

            WebElement exchange = webDriver.findElement(By.xpath("/html/body/div[1]/div[12]/div[1]/div/div/div[1]/div[1]/div/div[1]/div/div[1]/div/select"));
            Select select = new Select(exchange);
            select.selectByVisibleText("VN30");

            Thread.sleep(rd.nextInt(1000) + 1000);

            WebElement index = webDriver.findElement(By.xpath("/html/body/div[1]/div[12]/div[1]/div/div/div[1]/div[1]/div/div[1]/div/div[2]/div/span/span[1]/span/span[2]/b"));
            index.click();
            WebElement searchIndex = webDriver.findElement(By.xpath("/html/body/span/span/span[1]/input"));
            searchIndex.sendKeys(stock);
            searchIndex.sendKeys(Keys.ENTER);

            WebElement startD = webDriver.findElement(By.xpath("/html/body/div[1]/div[12]/div[1]/div/div/div[1]/div[1]/div/div[1]/div/div[3]/div/input"));
            startD.sendKeys(Keys.DELETE);
            startD.sendKeys(startDate);

            WebElement endD = webDriver.findElement(By.xpath("/html/body/div[1]/div[12]/div[1]/div/div/div[1]/div[1]/div/div[1]/div/div[4]/div/input"));
            endD.sendKeys(Keys.DELETE);
            endD.sendKeys(endDate);

            WebElement download = webDriver.findElement(By.xpath("/html/body/div[1]/div[12]/div[1]/div/div/div[1]/div[1]/div/div[2]/a/span"));
            download.click();
            Thread.sleep(rd.nextInt(1000) + 2000);
        }
    }

}
