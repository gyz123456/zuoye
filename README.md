# zuoye

作业1：使用mr按照要求排序

	代码位置 com.zuoye.hadoop.mr.sort.MRSortDriver


	步骤：

		1.map端将数值作为key输出。

		2.reduce端拉取数据的时候会对key进行排序。

		3.reduce方法中取出的key已经按照从小到大排列好了，只需要拼接上他是第几位就可以了。
