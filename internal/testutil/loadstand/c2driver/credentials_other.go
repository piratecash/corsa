//go:build !linux

package main

func readCredentials() (credentials, error) { return credentials{}, errRefused }
