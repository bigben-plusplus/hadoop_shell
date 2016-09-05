# -*- coding:utf-8 -*-
# 
import os, sys
import cmd
import subprocess
import shlex

class Shell(cmd.Cmd):
	def __init__(self):
		cmd.Cmd.__init__(self)

		self.prompt = '(hadoop) '

	def do_help(self, args):
		"pirnt help message"
		'List available commands with "help" or detailed help with "help cmd".'

		if args:
			try:
				func = getattr(self, 'help_' + args)
			except AttributeError:
				try:
					doc = getattr(self, 'do_' + args).__doc__
					if doc:
						self.stdout.write("%s\n\n" % str(doc))
						return
				except AttributeError:
					pass
				self.stdout.write("%s\n" % str("*** No help on %s" % (args,)))
				return
			func()
		else:			
			names = dir(self.__class__)

			cmds_doc = []
			cmds_undoc = []
			help = {}
			for name in names:
				if name[:5] == 'help_':
			 		help[name[5:]] = 1
			names.sort()
			prevname = ''
			for name in names:
				if name[:3] == 'do_':
					if name == prevname:
						continue
					prevname = name
					cmd = name[3:]
					if cmd in help:
						cmds_doc.append(cmd)
						del help[cmd]
					elif getattr(self, name).__doc__:
						cmds_doc.append(cmd)
					else:
						cmds_undoc.append(cmd)

			self.stdout.write("%s\n" % str(self.doc_leader))
			self.print_topics(self.doc_header,   cmds_doc,   15,80)
			self.print_topics(self.misc_header,  help.keys(),15,80)
			self.print_topics(self.undoc_header, cmds_undoc, 15,80)

	def help_hadoop(self):
		self.do_shell("hadoop --help")
	
	def help_version(self):
		print("print the version\n")

	def help_classpath(self):
		print("prints the class path needed to get the Hadoop jar and the required libraries\n")

	def help_checknative(self):
		self.do_shell("hadoop checknative -h")

	def help_distcp(self):
		self.do_shell("hadoop distcp")

	def help_archive(self):
		self.do_shell("hadoop archive")

	def help_credential(self):
		self.do_shell("hadoop credential")

	def help_daemonlog(self):
		self.do_shell("hadoop daemonlog")

	def help_trace(self):
		self.do_shell("hadoop trace")

	def help_fs(self):
		self.do_shell("hadoop fs")

	def help_jar(self):
		self.do_shell("hadoop jar")


	def do_version(self, args):
		"print the version"
		self.do_shell("hadoop version")

	def do_checknative(self, args):
		"check native hadoop and compression libraries availability"
		self.do_shell("hadoop checknative " + args)

	def do_classpath(self, args):
		"prints the class path needed to get the Hadoop jar and the required libraries"
		self.do_shell("hadoop classpath")

	def do_distcp(self, args):
		"copy file or directories recursively"
		self.do_shell("hadoop distcp " + args)

	def do_archive(self, args):
		"create a hadoop archive"
		self.do_shell("hadoop archive " + args)

	def do_credential(self, args):
		"interact with credential providers"
		self.do_shell("hadoop credential" + args)

	def do_daemonlog(self, args):
		"get/set the log level for each daemon"
		self.do_shell("hadoop daemonlog" + args)

	def do_trace(self, args):
		"view and modify Hadoop tracing settings"
		self.do_shell("hadoop trace" + args)

	def do_fs(self, args):
		"run a generic filesystem user client"
		self.do_shell("hadoop fs " + args)

	def do_jar(self, args):
		"run a jar file"
		self.do_shell("hadoop jar " + args)

	def do_sysctrl(self, args):
		"control hadoop cluster, setup, restart or shutdown the hadoop cluster"
		args = shlex.split(args);

		action   = "start"
		instance = "all"

		if (len(args) == 0):
			print("sysctrl <start|stop|restart> [all|dfs|yarn]")
			return

		# parse "action"
		if (len(args) >= 1):
			if (args[0] not in ("start", "restart", "stop")):
				self.stdout.write("%s\n" % \
					str("*** Invalid action %s. (start|restart|stop)" % (args[0],)))
				return

			action = args[0]

		# parse "instance"
		if (len(args) >= 2):
			if (args[1] not in ("all", "dfs", "yarn")):
				self.stdout.write("%s\n" % \
					str("*** Invalid instance %s. (all|dfs|yarn)" % (args[1],)))
				return

			instance = args[1]

		if action == "start":
			self.do_shell("start-" + instance + ".sh")
		elif action == "stop":
			self.do_shell("stop-" + instance + ".sh")
		elif action == "restart":
			self.do_shell("stop-" + instance + ".sh" + " && " "start-" + instance + ".sh")

	def do_start(self, args):
		"start hadoop cluster"
		self.do_sysctrl("start " + args)

	def do_restart(self, args):
		"restart hadoop cluster"
		self.do_sysctrl("restart " + args)

	def do_stop(self, args):
		"stop hadoop cluster"
		self.do_sysctrl("stop " + args)

	def do_status(self, args):
		"get hadoop cluster status"
		self.do_shell("jps")

	def do_shell(self, args):
		"run a shell commad"
		if not args is None:
			subshell = subprocess.Popen(args, shell=True, stdin=None, stdout=None)
			subshell.communicate()
			# subshell.terminate()
		print("")

	def do_quit(self, args):
		"exit hadoop shell"
		return True

if __name__ == "__main__":
	shell = Shell()

	try:
		shell.cmdloop("Hadoop Shell v 1.0.0\ndeveloped by bigben@seu.edu.cn\n")
	except KeyboardInterrupt as e:
		print "\nUser aborted"
	except Exception as e:
		print e

	print '\nBye!\n'

