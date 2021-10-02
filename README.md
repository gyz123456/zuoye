# zuoye

作业1：使用mr按照要求排序

	代码位置 com.zuoye.hadoop.mr.sort.MRSortDriver


	步骤：

		1.map端将数值作为key输出。

		2.reduce端拉取数据的时候会对key进行排序。

		3.reduce方法中取出的key已经按照从小到大排列好了，只需要拼接上他是第几位就可以了。
		
		
		

作业2：HQL

	代码位置 com.zuoye.hive
	
	3.2、如果两次浏览之间的间隔超过30分钟，认为是两个不同的浏览时间；再求每个id浏览时长、步长
	
		解题思路：
		
		1.使用lag函数求出与上一次浏览时间的时间差分钟数，如果大于30分钟标记为1
		
		2.对相同id的数据，开窗对标记求sum，这样可以给不同标记时间的数据打上标记
		
		3.浏览时长需要将第一次的浏览的时间间隔改为0
		
		4.对id和标记groupby即可得出结果
		


作业3：impala

代码位置 com.zuoye.impala

	解题思路：
	
     	 1.首先每个user_id, 获取当前点击的时间与上次点击的差额，如果大于30分钟，则标记为1
	
     	2.sum() over(partition by user_id order by click_time) ,给同一个会话内的数据赋予相同的标记 flag
	
    	3.再次开窗，进行排序 row_number() over(partition by user_id, flag order by click_time asc),删除多余字段就是结果
