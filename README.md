# final_project_ghtk

![alt text](https://github.com/NguyenLam101101/final_project_ghtk/blob/main/Architecture.png)
 
1. Crawl dữ liệu lịch sử giá chứng khoán của rổ chỉ số VN30 và đẩy vào kafka. Quá trình đẩy dữ liệu được lập lịch bằng airflow.
2. SparkStreaming đọc và xử lý dữ liệu từ kafka trước khi đẩy vào MySQL.
3. Đọc các bản ghi mới từ MySQL, thực hiện tính toán vào gửi vào API do PowerBI cung cấp.
4. PowerBI visualize dữ liệu.
