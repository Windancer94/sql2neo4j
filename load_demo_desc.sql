CREATE VIEW `n_res_class` AS select distinct `rhc_res`.`class_id` AS `pid`,`rhc_res`.`class_name` AS `name`,`rhc_res`.`class_id` AS `class_id`,`rhc_res`.`class_name` AS `class_name` from `rhc_res`;
CREATE VIEW `n_res_dept` AS select distinct `rhc_res`.`dept_id` AS `pid`,`rhc_res`.`dept_name` AS `name`,`rhc_res`.`dept_id` AS `dept_id`,`rhc_res`.`dept_name` AS `dept_name` from `rhc_res`;
CREATE VIEW `n_res_div` AS select distinct `rhc_res`.`div_id` AS `pid`,`rhc_res`.`div_name` AS `name`,`rhc_res`.`div_id` AS `div_id`,`rhc_res`.`div_name` AS `div_name` from `rhc_res`;
CREATE VIEW `n_res_subclass` AS select distinct `rhc_res`.`subclass_id` AS `pid`,`rhc_res`.`subclass_name` AS `name`,`rhc_res`.`subclass_id` AS `subclass_id`,`rhc_res`.`subclass_name` AS `subclass_name` from `rhc_res`;
CREATE VIEW `r_class_dept` AS select distinct `rhc_res`.`class_id` AS `pid`,`rhc_res`.`dept_id` AS `pid_e` from `rhc_res`;
CREATE VIEW `r_dept_div` AS select distinct `rhc_res`.`dept_id` AS `pid`,`rhc_res`.`div_id` AS `pid_e` from `rhc_res`;
CREATE VIEW `r_res_subclass` AS select distinct `rhc_res`.`sku_id` AS `pid`,`rhc_res`.`subclass_id` AS `pid_e` from `rhc_res`;
CREATE VIEW `r_subclass_class` AS select distinct `rhc_res`.`subclass_id` AS `pid`,`rhc_res`.`class_id` AS `pid_e` from `rhc_res`
