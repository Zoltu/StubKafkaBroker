package com.zoltu

import org.eclipse.jgit.api.Git
import org.eclipse.jgit.storage.file.FileRepositoryBuilder
import org.gradle.api.Plugin

import java.util.regex.Matcher
import org.gradle.api.Project

class VersioningPlugin implements Plugin<Project> {
	void apply(Project project) {
		project.ext.set("versionInfo", getVersionInfo(project.rootDir))
	}

	private VersionInfo getVersionInfo(File rootDirectory) {
		def repository = new FileRepositoryBuilder().findGitDir(rootDirectory).build()
		def git = Git.wrap(repository)
		def describeResults = git.describe().setLong(true).call()
		Matcher matcher = describeResults =~ /v([0-9]+)\.([0-9]+)\-([0-9]+)\-g(.*)/
		matcher.find()
		return  new VersionInfo(major: matcher.group(1), minor: matcher.group(2), patch: matcher.group(3), sha: matcher.group(4))
	}
}

class VersionInfo {
	String major
	String minor
	String patch
	String sha

	String toString() {
		return "${major}.${minor}.${patch}".toString()
	}
}
